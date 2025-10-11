# dags/japan_cars_lots.py

from datetime import datetime

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

from scripts.cars.common.telegram_alerts import build_failure_message, send_telegram_message, build_success_message
from scripts.cars.auctions.parser_auctions import ParserAuctions
from telegram_notifier import TelegramNotifier
from sent_dag_aummary import _send_dag_summary

local_tz = pendulum.timezone("Europe/Moscow")


def _run_parsing_auction(**context):
    brand_model_auc = Variable.get("auc_brand_model", default_var="honda/vezel")
    option_cars_auc = Variable.get("auc_option_cars", default_var="year-from=2013")
    batch_size = int(Variable.get("batch_size", default_var=20))
    min_year = int(Variable.get("min_year", default_var=2010))

    parser = ParserAuctions(
        airflow_mode=True,
        brand_model_auc=brand_model_auc,
        option_cars_auc=option_cars_auc,
        batch_size=batch_size,
        min_year=min_year
    )
    result = parser.parse_auctions_and_save()
    return result


def _on_failure_callback(context):
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    execution_date = context["execution_date"]
    exception = context.get("exception") or Exception("Неизвестная ошибка")

    message = build_failure_message(dag_id, task_id, execution_date, exception)
    send_telegram_message(message)


def _on_success_callback(context):
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    execution_date = context["execution_date"]

    # Получаем результат, если нужно
    result = context["task_instance"].xcom_pull(task_ids=task_id)
    extra = f"Обработано лотов: {result.get('total_lots', 'N/A')}" if result else ""

    start = context["dag_run"].start_date
    end = context["task_instance"].end_date or context["task_instance"].start_date
    duration = (end - start).total_seconds() if start else 0

    message = build_success_message(dag_id, task_id, execution_date, duration, extra)
    send_telegram_message(message)


with DAG(
        'japan_cars_auction',
        start_date=datetime(2025, 10, 11, tzinfo=local_tz),
        schedule_interval='0 10,13,16,19,22 * * *',
        # 10:00, 13:00, 16:00, 19:00, 22:00 — и на следующий день снова с 10:00
        catchup=False,
        tags=['japan', 'cars', 'auctions'],
) as dag:
    parse_and_load_auction = PythonOperator(
        task_id='parse_and_load_auction',
        python_callable=_run_parsing_auction,
        on_failure_callback=_on_failure_callback,
        on_success_callback=_on_success_callback,
    )

    # summary_task = PythonOperator(
    #     task_id='send_dag_summary',
    #     python_callable=_send_dag_summary,
    #     # Запускать ТОЛЬКО если parse_task успешен
    #     trigger_rule='all_success',
    # )
    #
    # parse_and_load_auction >> summary_task
