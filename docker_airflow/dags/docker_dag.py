from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.bash import BashOperator
from datetime import datetime,timedelta
from airflow.utils.dates import days_ago
from utils.notify import notify_on_failure

default_args = {
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    'on_failure_callback': notify_on_failure, # or list of functions
    # 'on_success_callback': some_other_function, # or list of functions
    # 'on_retry_callback': another_function, # or list of functions
    # 'sla_miss_callback': yet_another_function, # or list of functions
    # 'on_skipped_callback': another_function, #or list of functions
    # 'trigger_rule': 'all_success'
}

with DAG(
    dag_id="docker_dag", 
    default_args=default_args,
    description="Docker DAG",
    catchup=False,
    schedule=None,
    start_date=days_ago(2),
    tags=["example"],
) as dag:
    
    dataset_create = BashOperator(
        task_id="dataset_create",
        bash_command="echo 'Dataset create'",
    )

    train_model = DockerOperator(
        task_id="train_model",
        docker_url="tcp://docker-socket-proxy:2375",
        api_version="auto",
        auto_remove=True, # в случае True контейнер самовыпиливается после отработки ДАГА
        image="train_model:v1.0",
        container_name="train_model",
        environment={},
        #command=["python", "main_extend.py"], # если нам надо запустить другой скрипт без пересборки образа
    )

    dataset_create >> train_model