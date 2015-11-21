import hashlib
import json
import os
import requests

from pandas_td.notifier import BaseNotifier

class SlackNotifier(BaseNotifier):
    def __init__(self):
        self.author = os.environ.get('TD_USER')
        self.webhook_url = os.environ['TD_SLACK_WEBHOOK_URL']
        self.targets = os.environ.get('TD_SLACK_TARGETS')
        self.username = 'pandas-td'
        self.icon_url = 'https://avatars2.githubusercontent.com/u/747746'

    def post(self, message=None, attachment=None, notify=False):
        if notify and self.targets:
            message = self.targets + ': ' + message
        params = {
            'username': self.username,
            'icon_url': self.icon_url,
        }
        if message:
            params['text'] = message
            params['parse'] = 'full'
        if attachment:
            params['attachments'] = [attachment]
        r = requests.post(self.webhook_url, data={'payload': json.dumps(params)})
        r.raise_for_status()

    def post_message(self, status, message, text=None, notify=True):
        COLORS = {
            'info': 'good',
            'warning': 'warning',
            'error': 'danger',
        }
        attachment = None
        if text:
            attachment = {
                'color': COLORS[status],
                'title': text.split('\n')[0],
                'text': '\n'.join(text.split('\n')[1:]),
            }
        self.post(message, attachment=attachment, notify=notify)

    def post_task(self, task, notify=False):
        job = task.job
        status = job.status()
        if task.name:
            task_name = "{0}: Job ID {1}".format(task.name, task.job_id)
        else:
            task_name = "Job ID {0}".format(task.job_id)
        params = {
            'fallback': '{0} {1}'.format(task_name, status),
            'title': '{0} {1}'.format(task_name, status),
            'title_link': job.url,
            # the first line of query
            'text': job.query.split('\n')[0],
        }
        # author
        if self.author:
            digest = hashlib.md5(self.author.strip().lower().encode('ascii')).hexdigest()
            params['author_name'] = self.author
            params['author_icon'] = 'http://www.gravatar.com/avatar/' + digest
        fields = []
        # duration
        if task.job_start_at:
            job_duration = task.job_end_at - task.job_start_at
            fields.append({
                'title': 'Job Start',
                'value': str(task.job_start_at),
                'short': True,
            })
            fields.append({
                'title': 'Duration',
                'value': str(job_duration),
                'short': True,
            })
        # download
        if task.download_start_at and task.download_end_at:
            download_duration = task.download_end_at - task.download_start_at
            fields.append({
                'title': 'Download Start',
                'value': str(task.download_start_at),
                'short': True,
            })
            fields.append({
                'title': 'Duration',
                'value': str(download_duration),
                'short': True,
            })
        # fields
        params['fields'] = fields
        if status == 'running':
            params['color'] = 'warning'
        elif status == 'success':
            params['color'] = 'good'
        else:
            params['color'] = 'danger'
            params['text'] = job.debug['stderr']
        self.post(attachment=params)
