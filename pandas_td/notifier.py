import logging
logger = logging.getLogger(__name__)


class BaseNotifier(object):
    def notify(self, message, status, text):
        raise NotImplemented()

    def notify_tasks(self, message, tasks):
        raise NotImplemented()


class LoggingNotifier(BaseNotifier):
    '''
    Use logger for notifications.  This is not recommended because you will see
    unexpected outputs during your interactive sessions caused by background threads.
    '''

    def notify(self, message, status, text):
        if status == 'info':
            logger.info("%s", text)
        elif status == 'warning':
            logger.warning("%s", text)
        else:
            logger.error("%s", text)

    def notify_tasks(self, message, tasks):
        pass
