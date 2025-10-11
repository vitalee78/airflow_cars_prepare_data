# dags/japan_cars_lots.py

from datetime import datetime
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from scripts.cars.lots.parser_lots import ParserCars
from scripts.cars.common.telegram_alerts import send_telegram_message, build_failure_message, build_success_message
from sent_dag_aummary import _send_dag_summary

local_tz = pendulum.timezone("Europe/Moscow")


def _run_parsing_lots():
    brand_model = Variable.get("lot_brand_model", default_var="honda/vezel")
    option_cars = Variable.get("lot_option_cars", default_var="rate-4=4&rate-4-5=4.5&year-from=2016&year-to=2023")
    batch_size = int(Variable.get("batch_size", default_var=20))
    min_year = int(Variable.get("min_year", default_var=2010))

    parser = ParserCars(
        airflow_mode=True,
        brand_model=brand_model,
        option_cars=option_cars,
        batch_size=batch_size,
        min_year=min_year
    )
    result = parser.parse_tokidoki_and_save()
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
        'japan_cars_lots',
        start_date=datetime(2025, 10, 9, tzinfo=local_tz),
        schedule_interval='0 3 * * *',
        catchup=False,
        tags=['japan', 'cars', 'lots'],
) as dag:
    parse_and_load_lots = PythonOperator(
        task_id='parse_and_load_lots',
        python_callable=_run_parsing_lots,
        on_failure_callback=_on_failure_callback,
        on_success_callback=_on_success_callback,
    )

    summary_task = PythonOperator(
        task_id='send_dag_summary',
        python_callable=_send_dag_summary,
        # Запускать ТОЛЬКО если parse_task успешен
        trigger_rule='all_success',
    )

    parse_and_load_lots >> summary_task