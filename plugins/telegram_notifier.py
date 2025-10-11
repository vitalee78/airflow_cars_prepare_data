# $AIRFLOW_HOME/plugins/telegram_notifier.py

from __future__ import annotations

import requests
from airflow.notifications.basenotifier import BaseNotifier
from airflow.utils.context import Context
from airflow.models import Variable


class TelegramNotifier(BaseNotifier):
    def __init__(
        self,
        bot_token: str | None = None,
        chat_id: str | None = None,
        notify_success: bool = True,      # можно отключить успех
        notify_failure: bool = True,      # можно отключить ошибки
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.bot_token = bot_token or Variable.get("telegram_bot_token")
        self.chat_id = chat_id or Variable.get("telegram_chat_id")
        self.notify_success = notify_success
        self.notify_failure = notify_failure

    def notify(self, context: Context):
        task_instance = context["task_instance"]
        dag_id = context["dag"].dag_id
        task_id = task_instance.task_id
        execution_date = context["execution_date"]

        # Определяем, успех или ошибка
        exception = context.get("exception")
        if exception:
            if not self.notify_failure:
                return
            status = "❌ FAILED"
            error_msg = f"*Error:* `{str(exception)}`"
            message = (
                f"🚨 *Airflow Task {status}!*\n\n"
                f"*DAG:* `{dag_id}`\n"
                f"*Task:* `{task_id}`\n"
                f"*Execution Date:* `{execution_date}`\n"
                f"{error_msg}"
            )
        else:
            if not self.notify_success:
                return
            status = "✅ SUCCESS"
            message = (
                f"🎉 *Airflow Task {status}!*\n\n"
                f"*DAG:* `{dag_id}`\n"
                f"*Task:* `{task_id}`\n"
                f"*Execution Date:* `{execution_date}`\n"
                f"*Duration:* `{task_instance.duration:.2f} sec`"
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