=============
Notifications
=============

Pandas-TD supports several messaging services to get you notified when your queued jobs are completed.

.. image:: https://i.gyazo.com/b8c8ec775f8e91ad83265394d9816d34.png
   :width: 400 px

HipChat Notifications
=====================

.. image:: https://i.gyazo.com/bef6c398cf269dee72ab4c5e2f41bc06.png
   :width: 300 px

HipChat Send Notification Token
-------------------------------

1. Go to your HipChat group admin page (``https://<GROUP>.hipchat.com/admin/``).

2. Choose "Rooms" and select your favorite room.

3. Copy "API ID" in "Summary" tab.

4. Select "Tokens" tab and create a new token with "Send Notification" scope.

Configure HipChat Notification
------------------------------

Define the following environment variables::

    # Your email address
    export TD_USER="love-pandas@example.com"

    # Enable HipChat Notifier
    export TD_NOTIFIER_CLASS="pandas_td.drivers.hipchat.HipChatNotifier"

    # Room ID (API ID)
    export TD_HIPCHAT_ROOM_ID="12345678"

    # Send Notification token
    export TD_HIPCHAT_TOKEN="ubyr..."

    # (optional) Users to be notified
    export TD_HIPCHAT_TARGETS="@me, @td-users"

Slack Notifications
===================

.. image:: https://i.gyazo.com/9345b5efbf1535d8364cb898dfac9db2.png
   :width: 320 px

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
    export TD_NOTIFIER_CLASS="pandas_td.drivers.slack.SlackNotifier"

    # Webhook URL
    export TD_SLACK_WEBHOOK_URL="https://hooks.slack.com/services/..."

    # (optional) Users or groups to be notified
    export TD_SLACK_TARGETS="@me, @td-users"
