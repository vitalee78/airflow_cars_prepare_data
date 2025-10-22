import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.cars.ml.predict import Predict
from scripts.cars.ml.train_model import Train

from scripts.cars.common.telegram_alerts import build_failure_message, send_telegram_message, build_success_message

# Добавляем путь к проекту для импорта модулей
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

local_tz = pendulum.timezone("Asia/Novosibirsk")

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'ml-team',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def _train_model_task():
    """Задача обучения модели"""
    try:
        logger.info("Starting model training...")
        train = Train(airflow_mode=True, )
        train.train_and_evaluate()
        logger.info("Model training completed successfully")

        return "success"
    except Exception as e:
        logger.error(f"Model training failed: {e}")
        raise


def _predict_prices_task():
    """Задача прогнозирования цен"""
    try:
        config = {
            'start_price_ratio': 0.7,
            'min_start_price': 100000,
            'max_start_price': 5000000,
            'start_price_source': 'market',
            'filters': {
                'min_year': 2016,
                'max_year': 2023,
                'top': 5
            }
        }

        logger.info("Starting price predictions...")
        predict = Predict(airflow_mode=True)
        results = predict.predict_auction_prices(
            config=config,
            save_to_db=True,
            show_display=False
        )

        if results:
            logger.info(f"Price predictions completed. Processed {results['count_lots']} cars")
        else:
            logger.info("No cars processed")
        return results
    except Exception as e:
        logger.error(f"Price prediction failed: {e}")
        raise


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

    result = context["task_instance"].xcom_pull(task_ids=task_id, key="return_value")
    if not result or not isinstance(result, dict):
        extra = "Данные о результатах предсказаний отсутствуют."
    else:
        total = result.get("count_lots", 0)
        details = result.get("details", [])
        detail_lines = []
        for item in details:
            feature = item.get("feature_count", "N/A")
            count = item.get("count_lots", 0)
            detail_lines.append(f"• {feature}: {count} признаков")
        details_text = "\n".join(detail_lines) if detail_lines else "Нет деталей"
        extra = f"Всего обработано лотов: {total}\nПризнаков:\n{details_text}"

    start = context["dag_run"].start_date
    end = context["task_instance"].end_date or context["task_instance"].start_date
    duration = (end - start).total_seconds() if start else 0

    message = build_success_message(dag_id, task_id, execution_date, duration, extra)
    send_telegram_message(message)


with DAG(
        'ml_car_price_features',
        default_args=default_args,
        description='Генерация признаков для модели прогнозирования цены авто',
        schedule_interval='@daily',  # или @weekly, зависит от частоты аукционов
        start_date=datetime(2025, 10, 22, tzinfo=local_tz),
        catchup=False,
        tags=['ml', 'mart', 'car_prices', 'auction'],
) as dag:
    train_model = PythonOperator(
        task_id='train_model',
        python_callable=_train_model_task,
        on_failure_callback=_on_failure_callback,
    )

    predict_prices = PythonOperator(
        task_id='predict_prices',
        python_callable=_predict_prices_task,
        on_failure_callback=_on_failure_callback,
        on_success_callback=_on_success_callback,
    )

    train_model >> predict_prices
