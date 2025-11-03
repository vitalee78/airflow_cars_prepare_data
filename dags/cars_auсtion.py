# dags/cars_lots.py
import os
from datetime import datetime

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from scripts.cars.auctions.parser_auctions import ParserAuctions
from scripts.cars.common.telegram_alerts import build_failure_message, send_telegram_message, build_success_message
from scripts.cars.common.telegram_file_sender import send_csv_to_telegram

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


def _export_min_cost_cars_to_csv(**context):
    """Экспортирует fact.min_cost_cars_enriched в CSV."""
    hook = PostgresHook(postgres_conn_id='japan_cars_db')
    sql = "SELECT * FROM fact.min_cost_cars_enriched;"
    df = hook.get_pandas_df(sql)

    # Сохраняем в /tmp с датой
    csv_path = f"/tmp/min_cost_cars_{context['logical_date'].strftime('%Y-%m-%d_%H-%M-%S')}.csv"
    df.to_csv(csv_path, index=False, encoding='utf-8', sep=';')

    # Логируем размер
    size_kb = os.path.getsize(csv_path) / 1024
    print(f"Сохранён CSV: {csv_path} ({size_kb:.1f} KB)")

    return csv_path


def _cleanup_csv(**context):
    csv_path = context["task_instance"].xcom_pull(task_ids='export_min_cost_cars_csv')
    if csv_path and os.path.exists(csv_path):
        os.remove(csv_path)
        print(f"Временный файл удалён: {csv_path}")


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
        schedule_interval='0,16,19 * * * *',
        catchup=False,
        tags=['japan', 'cars', 'auctions'],
) as dag:
    parse_and_load_auction = PythonOperator(
        task_id='parse_and_load_auction',
        python_callable=_run_parsing_auction,
        on_failure_callback=_on_failure_callback,
        on_success_callback=_on_success_callback,
    )
    # . --exclude test_type:generic отключены тесты
    run_dbt_models = BashOperator(
        task_id='run_dbt_models',
        bash_command='''
            cd /home/ubuntu/airflow/airflow_home/dbt/dbt_cars_analytics &&
            unset PYTHONPATH &&
            set -a && source /etc/myapp/.env && set +a &&
            /home/ubuntu/projects/airflow/env/bin/dbt build --profiles-dir . --project-dir . --exclude test_type:generic
        ''',
        on_failure_callback=_on_failure_callback,
    )

    export_min_cost_cars_csv = PythonOperator(
        task_id='export_min_cost_cars_csv',
        python_callable=_export_min_cost_cars_to_csv,
        on_failure_callback=_on_failure_callback,
    )

    send_csv_to_telegram_task = PythonOperator(
        task_id='send_csv_to_telegram',
        python_callable=send_csv_to_telegram,
        on_failure_callback=_on_failure_callback,
    )

    cleanup_csv = PythonOperator(
        task_id='cleanup_csv',
        python_callable=_cleanup_csv,
    )

    restart_dash = BashOperator(
        task_id='restart_dash_service',
        bash_command='systemctl --user restart carapp',
        on_failure_callback=_on_failure_callback,
    )

    # Порядок выполнения
    (
            parse_and_load_auction
            >> run_dbt_models
            >> export_min_cost_cars_csv
            >> send_csv_to_telegram_task
            >> cleanup_csv
            >> restart_dash
    )
