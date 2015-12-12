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

from pandas_td.td import create_engine


class Session(object):
    def __init__(self, engine, name=None, callback=None):
        self.engine = engine
        self.index = None
        self.name = name
        self.callback = callback
        self.status = 'pending'
        self.created_at = None
        self.issued_at = None
        self.job_id = None
        self.job_start_at = None
        self.job_end_at = None
        self.job_future = None
        self.download_start_at = None
        self.download_end_at = None
        self.download_future = None
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
        self.sessions = []
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
            raise ImportError("invalid name: {0}".format(name))
        module = importlib.import_module(m.group(1))
        return getattr(module, m.group(2))()

    def __getitem__(self, i):
        return self.sessions[i]

    def now(self):
        return datetime.datetime.now(tz=tzlocal.get_localzone()).replace(microsecond=0)

    def localtime(self, dt):
        return dt.astimezone(tzlocal.get_localzone())

    def status(self):
        return pd.DataFrame([session.to_dict() for session in self.sessions],
                            columns=['created_at', 'status', 'job_id'])

    def wait(self):
        self.job_pool.shutdown()
        self.download_pool.shutdown()

    def submit_query(self, query, engine, name=None, callback=None, **kwargs):
        session = Session(engine, name=name, callback=callback)
        session.index = len(self.sessions)
        session.created_at = self.now()
        self.sessions.append(session)
        # schedule query
        session.job_future = self.job_pool.submit(self.do_query, session, query, kwargs)
        session.job_future.session = session
        session.job_future.add_done_callback(self.notification_callback)
        return session

    def submit_job(self, job_id, con, name=None, callback=None):
        job = con.client.job(job_id)
        if hasattr(job, 'database'):
            engine = create_engine('{}:{}'.format(job.type, job.database), con=con)
        else:
            # NOTE: tdclient <= 0.3.2 is broken
            engine = create_engine('{}:{}'.format(job.type, 'sample_datasets'), con=con)
        session = Session(engine, name=name, callback=callback)
        session.index = len(self.sessions)
        session.created_at = self.now()
        self.sessions.append(session)
        # schedule download
        session.job_id = job_id
        session.download_future = self.download_pool.submit(self.do_download, session, job)
        session.download_future.session = session
        session.download_future.add_done_callback(self.notification_callback)
        return session

    def do_query(self, session, query, kwargs):
        # parameters
        params = session.engine._params.copy()
        params.update(kwargs)

        # issue query
        client = session.engine.connection.get_client()
        job = client.query(session.engine.database, query, **params)
        session.status = 'queued'
        session.issued_at = self.now()
        session.job_id = job.job_id

        # wait
        job.wait(wait_interval=2)
        session.status = job.status()
        session.job_start_at = self.localtime(job._start_at)
        session.job_end_at = self.localtime(job._end_at)

        # status check
        if not job.success():
            return job.debug['stderr'] if job.debug else 'unkown error'

        # download
        session.download_future = self.download_pool.submit(self.do_download, session, job)
        session.download_future.session = session
        session.download_future.add_done_callback(self.notification_callback)

    def do_download(self, session, job):
        session.status = 'downloading'
        session.download_start_at = self.now()
        r = session.engine.get_result(job, wait=True)
        d = r.to_dataframe()
        session.status = 'downloaded'
        session.download_end_at = self.now()
        if session.callback:
            return session.callback(d)
        return d

    def notification_callback(self, future):
        session = future.session
        exc = future.exception()
        command = "{0}[{1}].result()".format(self.name or 'queue', session.index)

        # exceptions
        if exc is not None:
            title = type(exc).__name__
            if session.name:
                title = session.name + ': ' + title
            self.post_message('error',
                              'You got an exception.  Run `{0}` for details.'.format(command),
                              "{0}\n{1}".format(title, exc))
            session.status = 'exception'
            session.notified = True

        # errors
        if session.status in ['error', 'killed']:
            self.post_session(session)
            self.post_message('error', 'Query failed.  Run `{0}` for details.'.format(command))
            session.notified = True

        # success
        if session.status in ['downloaded']:
            self.post_session(session)

        # don't notify if there are pending or running sessions
        for session in self.sessions:
            if not session.done():
                return

        # all sessions are done
        notification_list = [session for session in self.sessions if not session.notified]
        if len(notification_list) > 0:
            queue = "'{0}'".format(self.name) if self.name else 'Queued'
            self.post_message('info', '{0} jobs completed.'.format(queue))
            for session in notification_list:
                session.notified = True

    def post_message(self, status, message, text=None):
        for notifier in self.notifiers:
            try:
                notifier.post_message(status, message, text)
            except:
                logger.error("%s", traceback.format_exc())

    def post_session(self, session):
        for notifier in self.notifiers:
            try:
                notifier.post_session(session)
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
