=============
Notifications
=============

Pandas-TD supports several messaging services to get you notified when queued jobs are completed.

.. image:: https://i.gyazo.com/9ae5a74167bd3a180e169403a68bd57b.png
   :width: 400 px

HipChat Notifications
=====================

HipChat Send Notification Token
-------------------------------

1. Go to your HipChat group admin page ("https://<GROUP>.hipchat.com/admin/").

2. Choose "Rooms" and select your favorite room.

3. Copy "API ID" in "Summary" tab.

4. Select "Tokens" tab and create a new token with "Send Notification" scope.  Then, copy "Token".

Configure HipChat Notification
------------------------------

Define the following environment variables::

    # Your email address
    export TD_USER="love-pandas@example.com"

    # Enable HipChat Notifier
    export TD_NOTIFIER_CLASS=pandas_td.drivers.hipchat.HipChatNotifier

    # Send Notification token
    export TD_HIPCHAT_TOKEN="ubyr..."

    # (optional) Users to be notified
    export TD_HIPCHAT_TARGETS="@k, @td-users"

Slack Notifications
===================

Slack Incoming WebHooks
-----------------------

1. Run Slack app and open your favorite channel.

2. Choose "Add a service integration..." from channel's menu.

3. Find "Incoming WebHooks", select a channel, and press "Add Incoming WebHooks Integration".

4. Copy "Webhook URL".

Configure Slack Notification
----------------------------

Define the following environment variables::

    # Your email address
    export TD_USER="love-pandas@example.com"

    # Enable Slack Notifier
    export TD_NOTIFIER_CLASS=pandas_td.drivers.slack.SlackNotifier

    # Webhook URL
    export TD_SLACK_WEBHOOK_URL="https://hooks.slack.com/services/..."

    # (optional) Users or groups to be notified
    export TD_SLACK_TARGETS="@k, @td-users"
