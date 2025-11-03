from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from sqlalchemy_utils.types.enriched_datetime.pendulum_date import pendulum

from scripts.cars.common.telegram_alerts import build_failure_message, send_telegram_message

local_tz = pendulum.timezone("Asia/Novosibirsk")

default_args = {
    'owner': 'data-pg',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def _on_failure_callback(context):
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    execution_date = context["execution_date"]
    exception = context.get("exception") or Exception("Неизвестная ошибка")

    message = build_failure_message(dag_id, task_id, execution_date, exception)
    send_telegram_message(message)


with DAG(
        'cleanup_tables_cars',
        default_args=default_args,
        description='Удаляет устаревшие записи из f_auction_cars',
        schedule_interval='0 1 * * *',
        start_date=datetime(2025, 10, 18, tzinfo=local_tz),
        catchup=True,
        tags=['cleanup', 'auction'],
) as dag:
    cleanup_task = PostgresOperator(
        task_id='delete_old_auction_lots',
        postgres_conn_id='japan_cars_db',
        sql="""
            CALL raw.sp_delete_old_auction_cars();
        """,
        on_failure_callback=_on_failure_callback,
    )
