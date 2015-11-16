===================
Slack Notifications
===================

"Slack notifications" allows you to get notified when your queued tasks are done.

.. image:: https://i.gyazo.com/9ae5a74167bd3a180e169403a68bd57b.png
   :width: 400 px

Slack Incoming WebHooks
=======================

1. Choose "Add a service integration..." from your preferred channel's menu in Slack app.

2. Find "Incoming WebHooks", select a channel, and press "Add Incoming WebHooks Integration".

3. Copy "Webhook URL".

Configuration
=============

Define the following environment variables::

    # Your email address
    export TD_USER="love-pandas@example.com"

    # Webhook URL
    export TD_SLACK_WEBHOOK_URL="https://hooks.slack.com/services/..."

    # (optional) Users or groups to be notified
    export TD_SLACK_TARGETS="@k, @td-users"
