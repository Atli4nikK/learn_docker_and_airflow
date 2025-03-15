"""
Пример DAG для проекта 1
"""
import os
import sys

# Импортируем общие утилиты
sys.path.append('/opt/airflow')
from common.utils import get_default_args, get_project_path, get_project_config

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Константы проекта
PROJECT_NAME = 'project1'
DAG_ID = f"{PROJECT_NAME}_example_dag"

# Получаем путь к проекту
project_path = get_project_path(PROJECT_NAME)

# Получаем конфигурацию проекта
project_config = get_project_config(PROJECT_NAME)


def example_task(**kwargs):
    """Пример задачи"""
    print(f"Выполняется задача для проекта {PROJECT_NAME}")
    print(f"Путь к проекту: {project_path}")
    print(f"Конфигурация проекта: {project_config}")
    return "Задача успешно выполнена"


# Создаем DAG
with DAG(
    DAG_ID,
    default_args=get_default_args(owner=PROJECT_NAME),
    description=f'Пример DAG для проекта {PROJECT_NAME}',
    schedule_interval='@daily',
    catchup=False,
    tags=[PROJECT_NAME]
) as dag:

    task1 = PythonOperator(
        task_id='example_task',
        python_callable=example_task,
        provide_context=True,
    )

    # Определение порядка выполнения задач
    task1 