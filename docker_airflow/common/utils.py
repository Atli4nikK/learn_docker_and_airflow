"""
Общие утилиты для всех DAG-ов
"""
import os
from datetime import datetime, timedelta

from airflow.models import Variable


def get_default_args(owner='airflow', retry_delay_minutes=5, retries=1):
    """
    Возвращает стандартные аргументы для DAG
    """
    return {
        'owner': owner,
        'depends_on_past': False,
        'start_date': datetime(2023, 1, 1),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': retries,
        'retry_delay': timedelta(minutes=retry_delay_minutes),
    }


def get_project_path(project_name):
    """
    Получает путь к папке проекта
    """
    return f"/opt/airflow/dags/{project_name}"


def get_project_config(project_name, config_key=None):
    """
    Получает конфигурацию проекта из переменных Airflow
    """
    try:
        config = Variable.get(f"{project_name}_config", deserialize_json=True)
        if config_key and config_key in config:
            return config[config_key]
        return config
    except Exception as e:
        print(f"Ошибка при получении конфигурации проекта {project_name}: {e}")
        return {} 