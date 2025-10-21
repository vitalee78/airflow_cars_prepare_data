from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PostgresOperator, PythonOperator

from cars.common.telegram_alerts import build_failure_message, send_telegram_message
from scripts.cars.ml.predict_model import PredictModel

local_tz = pendulum.timezone("Asia/Novosibirsk")

default_args = {
    'owner': 'ml-team',
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
        'ml_car_price_features',
        default_args=default_args,
        description='Генерация признаков для модели прогнозирования цены авто',
        schedule_interval='@daily',  # или @weekly, зависит от частоты аукционов
        start_date=datetime(2025, 10, 21, tzinfo=local_tz),
        catchup=True,
        tags=['ml', 'mart'],
) as dag:
    truncate = PostgresOperator(
        task_id='truncate_ml_table',
        sql="TRUNCATE TABLE mart.ml_car_price_features;",
        postgres_conn_id='japan_cars_db',
        on_failure_callback=_on_failure_callback,
    )

    build_features = PostgresOperator(
        task_id='build_ml_features',
        sql="""
            INSERT INTO mart.ml_car_price_features (
                id_car, auction_date, id_brand, id_model, id_carbody, year_release,
                rate, transmission, drive_type, fuel_type, mileage, start_price,
                market_avg_price_3m, market_median_price_3m, market_min_price_3m,
                market_max_price_3m, market_price_std_3m, market_listing_count_3m,
                market_avg_mileage_3m, mileage_vs_market_pct,
                target_price
            )
           SELECT ap.* FROM mart.v_ml_auction_price ap;
        """,
        postgres_conn_id='japan_cars_db',
        on_failure_callback=_on_failure_callback,
    )

    run_prediction = PythonOperator(
        task_id='run_price_prediction',
        python_callable=PredictModel.predict_and_save,
        on_failure_callback=_on_failure_callback,
    )

    truncate >> build_features >> run_prediction
