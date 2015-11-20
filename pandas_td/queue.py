import concurrent.futures
import datetime
import multiprocessing
import os
import pytz
import traceback
import tzlocal

import numpy as np
import pandas as pd

import logging
logger = logging.getLogger(__name__)

class QueryTask(object):
    def __init__(self, query, kwargs):
        self.query = query
        self.kwargs = kwargs
        self.task_id = None
        self.task_name = None
        self.status = None
        self.created_at = None
        self.issued_at = None
        self.job_id = None
        self.job_start_at = None
        self.job_end_at = None
        self.download_start_at = None
        self.download_end_at = None
        self.future = None
        self.notified = False

    def __repr__(self):
        return "<{0}.{1} task_id={2} task_name={3}>".format(
            self.__module__,
            self.__class__.__name__,
            self.task_id,
            self.task_name)

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
        return r

class JobQueue(object):
    def __init__(self, engine, name=None, workers=4):
        self.engine = engine
        self.name = name
        self.tasks = []
        self.job_pool = concurrent.futures.ThreadPoolExecutor(max_workers=workers)
        self.download_pool = concurrent.futures.ThreadPoolExecutor(max_workers=multiprocessing.cpu_count())
        # notifier
        if 'TD_SLACK_WEBHOOK_URL' in os.environ:
            from . import slack
            self.notifier = slack.SlackNotifier(self.get_client())
        else:
            self.notifier = None
            logging.warning('no notification method configured')

    def get_client(self):
        return self.engine.connection.get_client()

    def __getitem__(self, i):
        return self.tasks[i]

    def now(self):
        return datetime.datetime.now(tz=tzlocal.get_localzone()).replace(microsecond=0)

    def localtime(self, dt):
        return dt.astimezone(tzlocal.get_localzone())

    def status(self):
        return pd.DataFrame([task.to_dict() for task in self.tasks],
                            columns=['created_at', 'status', 'job_id'])

    def query(self, query, name=None, **kwargs):
        task = QueryTask(query, kwargs)
        task.task_id = len(self.tasks)
        task.task_name = name
        task.status = 'pending'
        task.created_at = self.now()
        self.tasks.append(task)
        # schedule query
        task.future = self.job_pool.submit(self.do_query, task)
        task.future.task = task
        task.future.add_done_callback(self.notification_callback)
        return task

    def download(self, job_id, name=None, **kwargs):
        job = self.get_client().job(job_id)
        task = QueryTask(job.query, kwargs)
        task.task_id = len(self.tasks)
        task.task_name = name
        task.job_id = job_id
        task.status = 'pending'
        task.created_at = self.now()
        self.tasks.append(task)
        # schedule download
        task.future = self.download_pool.submit(self.do_download, task, job)
        task.future.task = task
        task.future.add_done_callback(self.notification_callback)
        return task

    def do_query(self, task):
        # parameters
        params = self.engine._params.copy()
        params.update(task.kwargs)

        # issue query
        job = self.get_client().query(self.engine.database, task.query, **params)
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
        r = self.engine.get_result(job, wait=True)
        d = r.to_dataframe()
        task.status = 'downloaded'
        task.download_end_at = self.now()
        return d

    def notification_callback(self, future):
        task = future.task
        exc = future.exception()
        command = "{0}[{1}].result()".format(self.name or 'queue', task.task_id)

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

def create_queue(engine, name=None, workers=4):
    '''Create a job queue using a given query engine.

    The following environment variables can be used to contorol notifications:

      TD_USER                 Your email address
      TD_SLACK_WEBHOOK_URL    Slack Incoming Webhook URL
      TD_SLACK_TARGETS        (optional) Users or groups (e.g. "@td-users")

    Parameters
    ----------
    engine : QueryEngine
        Handler returned by connect. If not given, default connection is used.
    name : string, optional
        Queue name, used for notifications.
    workers : integer, default 4
        Use multiple workers for concurrent execution.

    Returns
    -------
    JobQueue
    '''
    return JobQueue(engine, name=name, workers=workers)
