import logging
logger = logging.getLogger(__name__)


class BaseNotifier(object):
    def post_message(self, status, message, text=None, notify=True):
        raise NotImplemented()

    def post_session(self, session, notify=False):
        raise NotImplemented()


class LoggingNotifier(BaseNotifier):
    '''
    Use Python logger for notifications.  This is not recommended because you will be
    interrupted by outputs from background threads during your interactive sessions.
    '''

    def post_message(self, status, message, text=None, notify=True):
        if status == 'info':
            logger.info("%s", message)
            if text:
                logger.info("%s", text)
        elif status == 'warning':
            logger.warning("%s", message)
            if text:
                logger.warning("%s", text)
        else:
            logger.error("%s", message)
            if text:
                logger.error("%s", text)

    def post_session(self, session, notify=False):
        # Don't confuse users by logging from background threads.
        # Users should manually check queue status.
        return
