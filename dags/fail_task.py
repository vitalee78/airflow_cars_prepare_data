from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from telegram_notifier import TelegramNotifier

def fail():
    raise Exception("Тест алерта!")

with DAG("test_telegram", start_date=datetime(2025, 10, 11), schedule_interval=None) as dag:
    PythonOperator(
        task_id="fail_task",
        python_callable=fail,
        on_failure_callback=TelegramNotifier()
    )