# scripts/db_postgres.py

import configparser
import json
import logging
import os
import platform
import sys
from typing import Optional

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger(__name__)


def _get_engine_from_config() -> Engine:
    """Создаёт engine из config.ini (для локального запуска)."""
    # Ищем config.ini в папке config на уровень выше scripts/
    config_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), '../../..', 'config', 'config.ini')
    )
    system = platform.system()
    if system == "Windows":
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Конфигурационный файл не найден: {config_path}")

    config = configparser.ConfigParser()
    config.read(config_path, encoding='utf-8')

    section = 'postgresTest'
    host = config.get(section, 'tHOST')
    port = config.get(section, 'tPORT')
    database = config.get(section, 'tDATABASE')
    user = config.get(section, 'tUSER')
    password = config.get(section, 'tPASSWORD')

    url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    return create_engine(url, echo=False)


def _get_engine_from_airflow(conn_id: str = 'japan_cars_db') -> Engine:
    """Создаёт engine через Airflow PostgresHook."""
    try:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id=conn_id)
        return hook.get_sqlalchemy_engine()
    except Exception as e:
        raise RuntimeError(f"Не удалось создать engine через Airflow Hook: {e}")


def get_engine(airflow_mode: Optional[bool] = None) -> Engine:
    """
    Универсальный engine:
      - в Airflow → через Hook,
      - локально → через config.ini.
    """
    if airflow_mode is True:
        return _get_engine_from_airflow()
    elif airflow_mode is False:
        return _get_engine_from_config()
    else:
        # Автоопределение: если есть airflow.models → режим Airflow
        try:
            from airflow.models.dag import DagContext
            if DagContext.get_current_dag():
                return _get_engine_from_airflow()
        except (ImportError, RuntimeError):
            pass
        return _get_engine_from_config()


def get_session(airflow_mode: Optional[bool] = None):
    engine = get_engine(airflow_mode=airflow_mode)
    Session = sessionmaker(bind=engine)
    return Session()


def check_isvalid_json(json_str):
    try:
        json.loads(json_str)
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON input: {e}")
        sys.exit(1)
