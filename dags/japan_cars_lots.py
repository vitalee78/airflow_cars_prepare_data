from datetime import datetime

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from scripts.cars.common.telegram_alerts import send_telegram_message, build_failure_message, build_success_message
from scripts.cars.lots.parser_lots import ParserCars

local_tz = pendulum.timezone("Europe/Moscow")


def _run_parsing_lots():
    brand_models = Variable.get("tokidoki_brand_models", deserialize_json=True)
    option_cars_list = Variable.get("tokidoki_option_cars", deserialize_json=True)
    batch_size = int(Variable.get("batch_size", default_var=20))
    min_year = int(Variable.get("min_year", default_var=2010))

    if not isinstance(brand_models, list) or not isinstance(option_cars_list, list):
        raise ValueError("Переменные tokidoki_brand_models и tokidoki_option_cars должны быть списками")

    if len(brand_models) != len(option_cars_list):
        raise ValueError("Списки brand_models и option_cars_list должны быть одинаковой длины")

    parser = ParserCars(
        airflow_mode=True,
        brand_models=brand_models,
        option_cars_list=option_cars_list,
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
