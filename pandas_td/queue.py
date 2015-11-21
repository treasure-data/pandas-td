import concurrent.futures
import datetime
import multiprocessing
import importlib
import os
import pytz
import re
import traceback
import tzlocal

import numpy as np
import pandas as pd

import logging
logger = logging.getLogger(__name__)


class BaseNotifier(object):
    def notify(self, message, status, text):
        raise NotImplemented()

    def notify_tasks(self, message, tasks):
        raise NotImplemented()


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
        self.download_start_at = None
        self.download_end_at = None
        self.future = None
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
        r = self.future.result()
        if self.status in ['error', 'killed']:
            logger.error('%s', r)
            return
        if type(r) is concurrent.futures.Future:
            r = r.result()
        return r

class JobQueue(object):
    def __init__(self, name=None, workers=4):
        self.name = name
        self.tasks = []
        self.job_pool = concurrent.futures.ThreadPoolExecutor(max_workers=workers)
        self.download_pool = concurrent.futures.ThreadPoolExecutor(max_workers=multiprocessing.cpu_count())
        self.notifier = self.get_notifier()

    def get_notifier(self):
        if 'TD_NOTIFIER_CLASS' in os.environ:
            name = os.environ['TD_NOTIFIER_CLASS']
        elif 'TD_HIPCHAT_APIKEY' in os.environ:
            name = 'pandas_td.drivers.hipchat.HipchatNotifier'
        elif 'TD_SLACK_WEBHOOK_URL' in os.environ:
            name = 'pandas_td.drivers.slack.SlackNotifier'
        else:
            logging.warning('no notification method defined')
            return None
        # load notifier
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
        task.future = self.job_pool.submit(self.do_query, task)
        task.future.task = task
        task.future.add_done_callback(self.notification_callback)
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
        task.future = self.download_pool.submit(self.do_download, task, job)
        task.future.task = task
        task.future.add_done_callback(self.notification_callback)
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
        future = self.download_pool.submit(self.do_download, task, job)
        future.task = task
        future.add_done_callback(self.notification_callback)
        return future

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
            self.notify('You got an exception.  Try `{0}` for details.'.format(command),
                        'error', "{0}: {1}".format(type(exc).__name__, exc))
            task.status = 'exception'
            task.notified = True

        # errors
        if task.status in ['error', 'killed']:
            self.notify_tasks('Query failed.  Try `{0}` for details.'.format(command), [task])
            task.notified = True

        # check the rest of tasks
        notification_list = []
        for task in [task for task in self.tasks if not task.notified]:
            # don't notify if there are pending or running tasks
            if task.future is None or not task.future.done():
                return
            notification_list.append(task)

        # all tasks are done
        if len(notification_list) > 0:
            queue = "'{0}'".format(self.name) if self.name else 'Queued'
            self.notify_tasks('{0} tasks completed.'.format(queue), notification_list)
            for task in notification_list:
                task.notified = True

    def notify(self, message, status, text):
        if self.notifier:
            try:
                self.notifier.notify(message, status, text)
            except:
                logger.error("%s", traceback.format_exc())
            return
        if status == 'info':
            logger.info("%s", text)
        elif status == 'warning':
            logger.warning("%s", text)
        else:
            logger.error("%s", text)

    def notify_tasks(self, message, tasks):
        if self.notifier:
            try:
                self.notifier.notify_tasks(message, tasks)
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
