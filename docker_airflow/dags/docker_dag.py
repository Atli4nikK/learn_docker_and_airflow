"""
DAG для запуска контейнеров Docker
"""
from datetime import datetime, timedelta

# Импорты Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
from docker.types import Mount

# Пользовательские модули
from utils.notify import notify_on_failure, notify_on_success

# --- КОНФИГУРАЦИЯ DAG ---
DAG_ID = "docker_dag"
DAG_DESCRIPTION = "Запуск задач в Docker-контейнерах"
DAG_SCHEDULE = None
DAG_CATCHUP = False
DAG_TAGS = ["example", "docker"]

# --- ОПРЕДЕЛЕНИЕ DAG ---
with DAG(
    DAG_ID,
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        'on_failure_callback': notify_on_failure,
    },
    description=DAG_DESCRIPTION,
    schedule=DAG_SCHEDULE,
    start_date=days_ago(2),
    catchup=DAG_CATCHUP,
    tags=DAG_TAGS,
) as dag:
    
    # --- ОПРЕДЕЛЕНИЕ ЗАДАЧ ---
    
    # Задача запуска контейнера PostgreSQL
    docker_postgres = DockerOperator(
        task_id='docker_postgres',
        image='postgres:14.0',
        container_name='docker_postgres',
        api_version='auto',
        auto_remove=True,
        environment={
            'POSTGRES_USER': 'postgres',
            'POSTGRES_PASSWORD': 'postgres',
            'POSTGRES_DB': 'postgres',
        },
        docker_url="tcp://docker-socket-proxy:2375",
        network_mode="bridge",
        command='postgres -c max_connections=100',
        doc_md="""
        ## Запуск PostgreSQL в Docker
        
        Эта задача запускает контейнер PostgreSQL для обработки данных.
        """,
    )
    
    # Задача запуска контейнера Python
    docker_python = DockerOperator(
        task_id='docker_python',
        image='python:3.10-slim',
        container_name='docker_python',
        api_version='auto',
        auto_remove=True,
        environment={
            'PYTHONUNBUFFERED': '1',
        },
        docker_url="tcp://docker-socket-proxy:2375",
        network_mode="bridge",
        command='python -c "import time; print(\'Processing data...\'); time.sleep(5); print(\'Processing complete!\')"',
        doc_md="""
        ## Запуск Python в Docker
        
        Эта задача запускает контейнер Python для обработки данных.
        """,
    )
    
    # Задача оповещения об успешном выполнении
    success = PythonOperator(
        task_id="success",
        python_callable=notify_on_success,
        doc_md="""
        ## Оповещение об успешном выполнении
        
        Отправляет уведомление об успешном выполнении DAG.
        """,
    )
    
    # --- ОПРЕДЕЛЕНИЕ ЗАВИСИМОСТЕЙ ---
    docker_postgres >> docker_python >> success
