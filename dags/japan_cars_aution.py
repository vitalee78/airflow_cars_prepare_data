# dags/japan_cars_lots.py

from datetime import datetime

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from scripts.cars.auctions.parser_auctions import ParserAuctions

local_tz = pendulum.timezone("Europe/Moscow")


def _run_parsing_auction():
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
    parser.parse_auctions_and_save()


with DAG(
        'japan_cars_auction',
        start_date=datetime(2025, 10, 11, tzinfo=local_tz),
        schedule_interval='0 10,13,16,19,22 * * *',
        # 10:00, 13:00, 16:00, 19:00, 22:00 — и на следующий день снова с 10:00
        catchup=False,
        tags=['japan', 'cars', 'auctions'],
) as dag:
    parse_and_load_task = PythonOperator(
        task_id='parse_and_load_auction',
        python_callable=_run_parsing_auction,
    )
