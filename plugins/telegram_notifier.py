# $AIRFLOW_HOME/plugins/telegram_notifier.py

from __future__ import annotations

import requests
from airflow.notifications.basenotifier import BaseNotifier
from airflow.utils.context import Context
from airflow.models import Variable


class TelegramNotifier(BaseNotifier):
    """
    Sends a message to Telegram when a DAG/task fails.
    Uses Airflow Variables: 'telegram_bot_token', 'telegram_chat_id'
    """

    def __init__(
        self,
        bot_token: str | None = None,
        chat_id: str | None = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.bot_token = bot_token or Variable.get("telegram_bot_token")
        self.chat_id = chat_id or Variable.get("telegram_chat_id")

    def notify(self, context: Context):
        task_instance = context["task_instance"]
        dag_id = context["dag"].dag_id
        task_id = task_instance.task_id
        execution_date = context["execution_date"]
        exception = context.get("exception", "Unknown error")

        message = (
            f"🚨 *Airflow Task Failed!*\n\n"
            f"*DAG:* `{dag_id}`\n"
            f"*Task:* `{task_id}`\n"
            f"*Execution Date:* `{execution_date}`\n"
            f"*Error:* `{str(exception)}`"
        )

        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": message,
            "parse_mode": "Markdown",
        }

        response = requests.post(url, data=payload)
        if not response.ok:
            raise RuntimeError(f"Failed to send Telegram message: {response.text}")