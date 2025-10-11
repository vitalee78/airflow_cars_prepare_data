# dags/japan_cars_lots.py

from datetime import datetime
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from scripts.cars.lots.parser_lots import ParserCars

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
    parser.parse_tokidoki_and_save()

with DAG(
    'japan_cars_lots',
    start_date=datetime(2025, 10, 9, tzinfo=local_tz),
    schedule_interval='0 3 * * *',  # 03:00 UTC = 06:00 MSK
    catchup=False,
    tags=['japan', 'cars'],
) as dag:

    parse_and_load_task = PythonOperator(
        task_id='parse_and_load_lots',
        python_callable=_run_parsing_lots,
    )