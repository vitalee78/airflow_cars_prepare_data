# dags/cars_lots.py

from datetime import datetime

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from scripts.cars.auctions.parser_auctions import ParserAuctions
from scripts.cars.common.telegram_alerts import build_failure_message, send_telegram_message, build_success_message

local_tz = pendulum.timezone("Asia/Novosibirsk")


def _run_parsing_auction(**context):
    brand_models = Variable.get("tokidoki_auc_brand_models", deserialize_json=True)
    option_cars_list = Variable.get("tokidoki_auc_option_cars", deserialize_json=True)
    batch_size = int(Variable.get("batch_size", default_var=20))
    min_year = int(Variable.get("min_year_auc", default_var=2014))

    parser = ParserAuctions(
        airflow_mode=True,
        brand_models=brand_models,
        option_cars_list=option_cars_list,
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

    # Получаем return_value из XCom
    result = context["task_instance"].xcom_pull(task_ids=task_id, key="return_value")

    if not result or not isinstance(result, dict):
        extra = "Данные о результатах отсутствуют."
    else:
        total = result.get("total_lots", 0)
        details = result.get("details", [])
        detail_lines = []
        for item in details:
            brand = item.get("brand_model", "N/A")
            count = item.get("lots_count", 0)
            detail_lines.append(f"• {brand}: {count} лотов")
        details_text = "\n".join(detail_lines) if detail_lines else "Нет деталей"
        extra = f"Всего обработано лотов: {total}\nПо брендам:\n{details_text}"

    start = context["dag_run"].start_date
    end = context["task_instance"].end_date or context["task_instance"].start_date
    duration = (end - start).total_seconds() if start else 0

    message = build_success_message(dag_id, task_id, execution_date, duration, extra)
    send_telegram_message(message)


with DAG(
        'japan_cars_auction',
        start_date=datetime(2025, 10, 11, tzinfo=local_tz),
        schedule_interval='0 10,16,19,22 * * *',
        catchup=False,
        tags=['japan', 'cars', 'auctions'],
) as dag:
    parse_and_load_auction = PythonOperator(
        task_id='parse_and_load_auction',
        python_callable=_run_parsing_auction,
        on_failure_callback=_on_failure_callback,
        on_success_callback=_on_success_callback,
    )

    run_dbt_models = BashOperator(
        task_id='run_dbt_models',
        bash_command=(
            'cd /home/ubuntu/projects/airflow/env/bin/activate '
            'export $(grep -v "^#" .env | xargs) && '
            'dbt build --profiles-dir . --project-dir .'
        ),
        on_failure_callback=_on_failure_callback,
        # env={  # Передаём базовые переменные, если нужно
        #     'PATH': '/home/ubuntu/airflow/airflow_home/env/bin:/usr/local/bin:/usr/bin:/bin'
        # }
    )

    restart_carapp = BashOperator(
        task_id='restart_carapp_service',
        bash_command='/home/ubuntu/airflow/airflow_home/scripts/restart_carapp.sh',
        on_failure_callback=_on_failure_callback,
    )

    # Порядок выполнения
    parse_and_load_auction >> run_dbt_models