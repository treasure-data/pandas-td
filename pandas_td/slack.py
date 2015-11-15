import hashlib
import json
import os
import requests

class SlackNotifier(object):
    def __init__(self, client):
        self.client = client
        self.webhook_url = os.environ['TD_SLACK_WEBHOOK_URL']
        self.targets = os.environ.get('TD_SLACK_TARGETS')
        self.author = os.environ.get('TD_USER')
        self.username = 'pandas-td'
        self.icon_url = 'https://avatars2.githubusercontent.com/u/747746'

    def post(self, message, attachments):
        if self.targets:
            message = self.targets + ': ' + message
        params = {
            'username': self.username,
            'icon_url': self.icon_url,
            'text': message,
            'parse': 'full',
            'attachments': attachments,
        }
        r = requests.post(self.webhook_url, data={'payload': json.dumps(params)})
        r.raise_for_status()

    def notify(self, message, status, text):
        COLORS = {
            'info': 'good',
            'warning': 'warning',
            'error': 'danger',
        }
        attachment = {
            'color': COLORS[status],
            'text': text,
        }
        self.post(message, attachments=[attachment])

    def notify_tasks(self, message, tasks):
        self.post(message, attachments=[self.task_attachment(task) for task in tasks])

    def task_attachment(self, task):
        job = self.client.job(task.job_id)
        if task.task_name:
            task_name = "{0}: Job ID {1}".format(task.task_name, task.job_id)
        else:
            task_name = "Job ID {0}".format(task.job_id)
        params = {}
        fields = []
        # author
        if self.author:
            digest = hashlib.md5(self.author.strip().lower().encode('ascii')).hexdigest()
            params['author_name'] = self.author
            params['author_icon'] = 'http://www.gravatar.com/avatar/' + digest
        # link
        params['title_link'] = job.url
        # query text
        params['text'] = job.query.split('\n')[0]
        # duration
        if task.job_start_at:
            job_duration = task.job_end_at - task.job_start_at
            fields.append({'title': 'Job Start', 'value': str(task.job_start_at), 'short': True})
            fields.append({'title': 'Duration', 'value': str(job_duration), 'short': True})
        # download
        if task.download_start_at and task.download_end_at:
            download_duration = task.download_end_at - task.download_start_at
            fields.append({'title': 'Download Start', 'value': str(task.download_start_at), 'short': True})
            fields.append({'title': 'Duration', 'value': str(download_duration), 'short': True})
        # fields
        params['fields'] = fields
        # status
        status = job.status()
        if status == 'running':
            params['color'] = 'warning'
            params['fallback'] = '{0} is still running'.format(task_name)
            params['title'] = '{0} still running'.format(task_name)
        elif status == 'success':
            params['color'] = 'good'
            params['fallback'] = '{0} {1}'.format(task_name, status)
            params['title'] = '{0} {1}'.format(task_name, status)
            params['text'] = job.query.split('\n')[:3]
        else:
            params['color'] = 'danger'
            params['fallback'] = '{0} {1}'.format(task_name, status)
            params['title'] = '{0} {1}'.format(task_name, status)
            params['text'] = job.debug['stderr']
        return params
