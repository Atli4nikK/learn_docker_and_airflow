"""
DAG для работы с Amazon S3
"""
import json
import logging
import os
import datetime as dt
from datetime import datetime, timedelta

# Импорты Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.utils.dates import days_ago
from docker.types import Mount

# --- КОНФИГУРАЦИЯ DAG ---
DAG_ID = "s3_dag"
DAG_DESCRIPTION = "Работа с данными в Amazon S3"
DAG_SCHEDULE = None
DAG_CATCHUP = False
DAG_TAGS = ["example", "s3"]

# Параметры для работы с S3
BUCKET_NAME = 'mydatabucket'
KEY = 'my-key.csv'
LOCAL_FILE_PATH = '/tmp/processed_data.csv'
AWS_CONN_ID = 's3_connection'

# Параметры для Docker
keyword = "new_keyword"
LOCAL_DATE = datetime.now().strftime("%Y-%m-%d")

# --- ОПРЕДЕЛЕНИЕ ФУНКЦИЙ ---
def upload_to_s3(**context):
    """
    Загружает файл в Amazon S3
    
    Args:
        **context: Контекст выполнения задачи Airflow
    """
    logging.info(f"Uploading file {LOCAL_FILE_PATH} to S3")
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    s3_hook.load_file(
        filename=LOCAL_FILE_PATH,
        key=f"{LOCAL_DATE}/{KEY}",
        bucket_name=BUCKET_NAME,
        replace=True
    )
    logging.info("File uploaded successfully")

# --- ОПРЕДЕЛЕНИЕ DAG ---
with DAG(
    DAG_ID,
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description=DAG_DESCRIPTION,
    schedule_interval=DAG_SCHEDULE,
    start_date=days_ago(1),
    catchup=DAG_CATCHUP,
    tags=DAG_TAGS,
) as dag:
    
    # --- ОПРЕДЕЛЕНИЕ ЗАДАЧ ---
    
    # Проверка наличия ключа в S3
    check_file_sensor = S3KeySensor(
        task_id='check_for_file_in_s3',
        bucket_key=f"{LOCAL_DATE}/{KEY}",
        bucket_name=BUCKET_NAME,
        aws_conn_id=AWS_CONN_ID,
        poke_interval=60,  # каждую минуту
        timeout=60 * 60,  # таймаут 1 час
        mode='poke',
        doc_md="""
        ## Проверка наличия файла в S3
        
        Эта задача проверяет наличие файла в бакете S3.
        """,
    )
    
    # Загрузка файла в S3
    upload_to_s3_task = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        provide_context=True,
        doc_md="""
        ## Загрузка файла в S3
        
        Эта задача загружает локальный файл в бакет S3.
        """,
    )
    
    # Обработка файла с помощью Docker
    train_model = DockerOperator(
        task_id='train_model',
        image='train-model-image:latest',
        docker_url="tcp://docker-socket-proxy:2375",
        api_version='auto',
        auto_remove=True,
        environment={
            'KEYWORD': keyword,
            'DATE': LOCAL_DATE,
        },
        mounts=[
            Mount(
                source=f"C:/Users/koldyrkaev/Desktop/python/airflow1/learn_docker_and_airflow/docker_airflow/dags/data/{keyword}",
                target="/app/images",
                type="bind"
            ),
            Mount(
                source=f"C:/Users/koldyrkaev/Desktop/python/airflow1/learn_docker_and_airflow/docker_airflow/dags/train_model/no_cars/{keyword}",
                target="/app/no_cars",
                type="bind"
            ),
            Mount(
                source="C:/Users/koldyrkaev/Desktop/python/airflow1/learn_docker_and_airflow/docker_airflow/dags/train_model/check.txt",
                target="/app/check.txt",
                type="bind"
            )
        ],
        command=f"python train.py",
        network_mode="bridge",
        doc_md="""
        ## Запуск модели в Docker
        
        Эта задача запускает Docker-контейнер для обучения модели на данных.
        """,
    )
    
    # --- ОПРЕДЕЛЕНИЕ ЗАВИСИМОСТЕЙ ---
    check_file_sensor >> upload_to_s3_task >> train_model
