import logging
import os
import requests
from airflow.models import Variable

logger = logging.getLogger(__name__)


def send_csv_to_telegram(**context):
    """
    Отправляет CSV-файл в Telegram как документ.
    Ожидает путь к файлу из XCom задачи 'export_min_cost_cars_csv'.
    """
    bot_token = Variable.get("telegram_bot_token")
    chat_id = Variable.get("telegram_chat_id")

    csv_path = context["task_instance"].xcom_pull(task_ids='export_min_cost_cars_csv')
    if not csv_path or not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV не найден: {csv_path}")

    # Готовим имя файла
    filename = f"min_cost_cars_{context['ds']}.csv"

    # Отправляем документ
    url = f"https://api.telegram.org/bot{bot_token}/sendDocument"
    with open(csv_path, 'rb') as f:
        files = {'document': (filename, f)}
        data = {'chat_id': chat_id, 'caption': f"📄 Отчёт по минимальным ценам за {context['ds']}"}
        response = requests.post(url, files=files, data=data)

    if response.status_code != 200:
        error = response.json().get("description", "Неизвестная ошибка")
        raise Exception(f"Ошибка Telegram API: {error}")

    logger.info("Файл успешно отправлен в Telegram")
