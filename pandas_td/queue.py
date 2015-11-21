import concurrent.futures
import datetime
import multiprocessing
import importlib
import os
import pytz
import re
import six
import traceback
import tzlocal

import numpy as np
import pandas as pd

import logging
logger = logging.getLogger(__name__)


class QueryTask(object):
    def __init__(self, query, engine, kwargs):
        self.query = query
        self.engine = engine
        self.kwargs = kwargs
        self.index = None
        self.name = None
        self.status = None
        self.created_at = None
        self.issued_at = None
        self.job_id = None
        self.job_start_at = None
        self.job_end_at = None
        self.job_future = None
        self.download_start_at = None
        self.download_end_at = None
        self.download_future = None
        self.callback = None
        self.notified = False

    @property
    def job(self):
        if not self.job_id:
            raise AttributeError()
        return self.engine.connection.get_client().job(self.job_id)

    def __repr__(self):
        return "<{0}.{1} index={2} name={3} status={4}>".format(
            self.__module__,
            self.__class__.__name__,
            self.index,
            self.name,
            self.status)

    def done(self):
        if self.notified:
            return True
        if (self.job_future is None) or (not self.job_future.done()):
            return False
        if (self.download_future is None) or (not self.download_future.done()):
            return False
        return True

    def to_dict(self):
        return {
            'status': self.status,
            'job_id': self.job_id,
            'created_at': self.created_at,
            'issued_at': self.issued_at,
            'job_start_at': self.job_start_at,
            'job_end_at': self.job_end_at,
            'download_start_at': self.download_start_at,
            'download_end_at': self.download_end_at,
        }

    def result(self):
        if self.job_future:
            r = self.job_future.result()
            if isinstance(r, six.string_types):
                logger.error('%s', r)
                return
        if self.download_future:
            return self.download_future.result()
        # cannot happen
        raise ValueError('not started yet')


class JobQueue(object):
    def __init__(self, name=None, workers=4):
        self.name = name
        self.tasks = []
        self.job_pool = concurrent.futures.ThreadPoolExecutor(max_workers=workers)
        self.download_pool = concurrent.futures.ThreadPoolExecutor(max_workers=multiprocessing.cpu_count())
        self.notifiers = list(self.get_notifiers())
        if len(self.notifiers) == 0:
            logging.warning('no notification method defined')
            self.notifiers.append(self.get_notifier('pandas_td.notifier.LoggingNotifier'))

    def get_notifiers(self):
        # Bundled classes:
        # - pandas_td.notifier.LoggingNotifier
        # - pandas_td.drivers.hipchat.HipChatNotifier
        # - pandas_td.drivers.slack.SlackNotifier
        if 'TD_NOTIFIER_CLASS' in os.environ:
            yield self.get_notifier(os.environ['TD_NOTIFIER_CLASS'])

    def get_notifier(self, name):
        logger.info('initializing notifier: %s', name)
        m = re.match('(.*)\\.([^.]+)', name)
        if not m:
            raise ImportError("import failed: {0}".format(name))
        module = importlib.import_module(m.group(1))
        return getattr(module, m.group(2))()

    def __getitem__(self, i):
        return self.tasks[i]

    def now(self):
        return datetime.datetime.now(tz=tzlocal.get_localzone()).replace(microsecond=0)

    def localtime(self, dt):
        return dt.astimezone(tzlocal.get_localzone())

    def status(self):
        return pd.DataFrame([task.to_dict() for task in self.tasks],
                            columns=['created_at', 'status', 'job_id'])

    def query(self, query, engine, name=None, callback=None, **kwargs):
        task = QueryTask(query, engine, kwargs)
        task.index = len(self.tasks)
        task.name = name
        task.status = 'pending'
        task.callback = callback
        task.created_at = self.now()
        self.tasks.append(task)
        # schedule query
        task.job_future = self.job_pool.submit(self.do_query, task)
        task.job_future.task = task
        task.job_future.add_done_callback(self.notification_callback)
        return task

    def download(self, job_id, engine, name=None, callback=None, **kwargs):
        job = self.get_client().job(job_id)
        task = QueryTask(job.query, engine, kwargs)
        task.index = len(self.tasks)
        task.name = name
        task.job_id = job_id
        task.status = 'pending'
        task.callback = callback
        task.created_at = self.now()
        self.tasks.append(task)
        # schedule download
        task.download_future = self.download_pool.submit(self.do_download, task, job)
        task.download_future.task = task
        task.download_future.add_done_callback(self.notification_callback)
        return task

    def do_query(self, task):
        # parameters
        params = task.engine._params.copy()
        params.update(task.kwargs)

        # issue query
        client = task.engine.connection.get_client()
        job = client.query(task.engine.database, task.query, **params)
        task.status = 'queued'
        task.issued_at = self.now()
        task.job_id = job.job_id

        # wait
        job.wait(wait_interval=2)
        task.status = job.status()
        task.job_start_at = self.localtime(job._start_at)
        task.job_end_at = self.localtime(job._end_at)

        # status check
        if not job.success():
            return job.debug['stderr'] if job.debug else 'unkown error'

        # download
        task.download_future = self.download_pool.submit(self.do_download, task, job)
        task.download_future.task = task
        task.download_future.add_done_callback(self.notification_callback)

    def do_download(self, task, job=None):
        task.status = 'downloading'
        task.download_start_at = self.now()
        r = task.engine.get_result(job, wait=True)
        d = r.to_dataframe()
        task.status = 'downloaded'
        task.download_end_at = self.now()
        if task.callback:
            return task.callback(d)
        return d

    def notification_callback(self, future):
        task = future.task
        exc = future.exception()
        command = "{0}[{1}].result()".format(self.name or 'queue', task.index)

        # exceptions
        if exc is not None:
            title = type(exc).__name__
            if task.name:
                title = task.name + ': ' + title
            self.post_message('error',
                              'You got an exception.  Run `{0}` for details.'.format(command),
                              "{0}\n{1}".format(title, exc))
            task.status = 'exception'
            task.notified = True

        # errors
        if task.status in ['error', 'killed']:
            self.post_task(task)
            self.post_message('error', 'Query failed.  Run `{0}` for details.'.format(command))
            task.notified = True

        # success
        if task.status in ['downloaded']:
            self.post_task(task)

        # don't notify if there are pending or running tasks
        for task in self.tasks:
            if not task.done():
                return

        # all tasks are done
        notification_list = [task for task in self.tasks if not task.notified]
        if len(notification_list) > 0:
            queue = "'{0}'".format(self.name) if self.name else 'Queued'
            self.post_message('info', '{0} jobs completed.'.format(queue))
            for task in notification_list:
                task.notified = True

    def post_message(self, status, message, text=None):
        for notifier in self.notifiers:
            try:
                notifier.post_message(status, message, text)
            except:
                logger.error("%s", traceback.format_exc())

    def post_task(self, task):
        for notifier in self.notifiers:
            try:
                notifier.post_task(task)
            except:
                logger.error("%s", traceback.format_exc())

def create_queue(name=None, workers=4):
    '''Create a job queue.

    The following environment variables are used to contorol notifications:

      TD_USER                 Your email address
      TD_SLACK_WEBHOOK_URL    Slack Incoming Webhook URL
      TD_SLACK_TARGETS        (optional) Users or groups (e.g. "@td-users")

    Parameters
    ----------
    name : string, optional
        Queue name, used for notifications.
    workers : integer, default 4
        Use multiple workers for concurrent execution.

    Returns
    -------
    JobQueue
    '''
    return JobQueue(name=name, workers=workers)
