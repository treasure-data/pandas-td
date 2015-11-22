import hashlib
import os
import requests
import uuid

from pandas_td.notifier import BaseNotifier

class HipChatNotifier(BaseNotifier):
    def __init__(self):
        self.author = os.environ.get('TD_USER')
        self.room_id = os.environ['TD_HIPCHAT_ROOM_ID']
        self.token = os.environ['TD_HIPCHAT_TOKEN']
        self.targets = os.environ.get('TD_HIPCHAT_TARGETS')

    def post(self, message, card=None, color=None, notify=False):
        params = {
            'notify': notify,
        }
        if self.author:
            params['from'] = self.author[:25]
        if notify and self.targets:
            message = self.targets + ': ' + message
        if card:
            message += '\n[card attached]'
        params['message'] = message
        params['message_format'] = 'text'
        if card:
            card['id'] = uuid.uuid4().urn
            params['card'] = card
        if color:
            params['color'] = color
        headers = {
            'Authorization': 'Bearer ' + self.token,
        }
        r = requests.post('https://api.hipchat.com/v2/room/{0}/notification'.format(self.room_id),
                          json = params,
                          headers = headers)
        r.raise_for_status()

    def post_message(self, status, message, text=None, notify=True):
        COLORS = {
            'info': 'green',
            'warning': 'yellow',
            'error': 'red',
        }
        if text:
            card = {
                'style': 'application',
                'format': 'medium',
                'title': text.split('\n')[0],
                'description': '\n'.join(text.split('\n')[1:]),
            }
            self.post(text, card=card, color=COLORS[status])
        self.post(message, color=COLORS[status], notify=notify)

    def post_session(self, session, notify=False):
        job = session.job
        status = job.status()
        if session.name:
            title = "{0}: Job ID {1} {2}".format(session.name, session.job_id, status)
        else:
            title = "Job ID {0} {1}".format(session.job_id, status)
        params = {
            'style': 'application',
            'format': 'medium',
            'title': title,
            'url': job.url,
            # the first line of query
            'description': job.query.split('\n')[0],
        }
        # author
        if self.author:
            digest = hashlib.md5(self.author.encode('ascii')).hexdigest()
            params['icon'] = {'url': 'http://www.gravatar.com/avatar/' + digest}
        attributes = []
        # duration
        if session.job_start_at:
            job_duration = session.job_end_at - session.job_start_at
            attributes.append({
                'label': 'Job Start',
                'value': {'label': str(session.job_start_at), 'style': 'lozenge'},
            })
            attributes.append({
                'label': 'Duration',
                'value': {'label': str(job_duration), 'style': 'lozenge'},
            })
        # download
        if session.download_start_at and session.download_end_at:
            download_duration = session.download_end_at - session.download_start_at
            attributes.append({
                'label': 'Download Start',
                'value': {'label': str(session.download_start_at), 'style': 'lozenge'},
            })
            attributes.append({
                'label': 'Duration',
                'value': {'label': str(download_duration), 'style': 'lozenge'},
            })
        # attributes
        params['attributes'] = attributes
        if status == 'running':
            color = 'yellow'
        elif status == 'success':
            color = 'green'
        else:
            color = 'red'
            params['description'] = job.debug['stderr']
        self.post(params['title'], card=params, color=color)
