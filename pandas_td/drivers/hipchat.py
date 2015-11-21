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

    def post(self, message=None, card=None, color=None, notify=False):
        params = {
            'notify': notify,
        }
        if self.author:
            params['from'] = self.author[:25]
        if message:
            if self.targets:
                message = self.targets + ': ' + message
            params['message'] = message
            params['message_format'] = 'text'
        else:
            params['message'] = 'attached:'
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

    def notify(self, message, status, text):
        COLORS = {
            'info': 'green',
            'warning': 'yellow',
            'error': 'red',
        }
        card = {
            'style': 'application',
            'format': 'medium',
            'title': 'Exception',
            'description': text,
            'activity': {
                'html': text,
            }
        }
        self.post(message=message, color=COLORS[status], notify=True)
        self.post(card=card, color=COLORS[status])

    def notify_tasks(self, message, tasks):
        for task in tasks:
            self.notify_task(task)
        self.post(message, notify=True)

    def notify_task(self, task):
        job = task.job
        status = job.status()
        if task.name:
            task_name = "{0}: Job ID {1}".format(task.name, task.job_id)
        else:
            task_name = "Job ID {0}".format(task.job_id)
        params = {
            'style': 'application',
            'format': 'medium',
            'title': '{0} {1}'.format(task_name, status),
            'url': job.url,
            # the first line of query 
            'description': job.query.split('\n')[0],
        }
        if self.author:
            digest = hashlib.md5(self.author.encode('ascii')).hexdigest()
            params['icon'] = {'url': 'http://www.gravatar.com/avatar/' + digest}
        attributes = []
        # duration
        if task.job_start_at:
            job_duration = task.job_end_at - task.job_start_at
            attributes.append({
                'label': 'Job Start',
                'value': {'label': str(task.job_start_at), 'style': 'lozenge'},
            })
            attributes.append({
                'label': 'Duration',
                'value': {'label': str(job_duration), 'style': 'lozenge'},
            })
        # download
        if task.download_start_at and task.download_end_at:
            download_duration = task.download_end_at - task.download_start_at
            attributes.append({
                'label': 'Download Start',
                'value': {'label': str(task.download_start_at), 'style': 'lozenge'},
            })
            attributes.append({
                'label': 'Duration',
                'value': {'label': str(download_duration), 'style': 'lozenge'},
            })
        # attributes
        params['attributes'] = attributes
        # status
        status = job.status()
        if status == 'running':
            color = 'yellow'
            params['title'] = '{0} still running'.format(task_name)
        elif status == 'success':
            color = 'green'
        else:
            color = 'red'
            params['description'] = job.debug['stderr']
        self.post(card=params, color=color)
