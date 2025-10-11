# scripts/utils/telegram_alerts.py

from typing import Optional

import requests
from airflow.models import Variable


def send_telegram_message(
        message: str,
        chat_id: Optional[str] = None,
        bot_token: Optional[str] = None
) -> None:
    """Универсальная функция отправки сообщения в Telegram."""
    if bot_token is None:
        bot_token = Variable.get("telegram_bot_token")
    if chat_id is None:
        chat_id = Variable.get("telegram_chat_id")

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown",
    }
    response = requests.post(url, data=payload)
    if not response.ok:
        raise RuntimeError(f"Ошибка отправки в Telegram: {response.text}")


def build_success_message(
        dag_id: str,
        task_id: str,
        execution_date,
        duration_sec: float,
        extra_info: str = ""
) -> str:
    mins, secs = divmod(int(duration_sec), 60)
    msg = (
        f"✅ *Успешно завершено!*\n\n"
        f"*DAG:* `{dag_id}`\n"
        f"*Task:* `{task_id}`\n"
        f"*Дата:* `{execution_date}`\n"
        f"*Время:* `{mins} мин {secs} сек`"
    )
    if extra_info:
        msg += f"\n*Детали:* {extra_info}"
    return msg


def build_failure_message(
        dag_id: str,
        task_id: str,
        execution_date,
        exception: Exception
) -> str:
    return (
        f"🚨 *Ошибка в Airflow!*\n\n"
        f"*DAG:* `{dag_id}`\n"
        f"*Task:* `{task_id}`\n"
        f"*Дата:* `{execution_date}`\n"
        f"*Ошибка:* `{str(exception)}`"
    )
