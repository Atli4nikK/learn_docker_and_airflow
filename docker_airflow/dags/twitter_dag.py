"""
DAG для получения данных из Twitter
"""
from datetime import datetime, timedelta

# Импорты Airflow
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Пользовательские модули
from common.notify import notify_on_failure
from utils.twitter_etl import twitter_etl

# --- КОНФИГУРАЦИЯ DAG ---
DAG_ID = "twitter_dag"
DAG_DESCRIPTION = "Получение данных из Twitter API"
DAG_SCHEDULE = None
DAG_CATCHUP = False
DAG_TAGS = ["koldyrkaevs"]

# --- ОПРЕДЕЛЕНИЕ ФУНКЦИЙ ---
def get_params(**context):
    """
    Получает параметры из контекста DAG Run
    
    Args:
        **context: Контекст выполнения задачи Airflow
    """
    params = context['dag_run'].conf

# --- ОПРЕДЕЛЕНИЕ DAG ---
with DAG(
    DAG_ID,
    # Аргументы по умолчанию для всех задач
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
    
    # Задача для получения параметров
    get_parameters = PythonOperator(
        task_id="get_parameters",
        python_callable=get_params,
        doc_md="""
        ## Получение параметров
        
        Эта задача получает параметры из контекста DAG Run.
        """,
    )

    # Python-задача для получения данных из Twitter
    twitter_task = PythonOperator(
        task_id="python_task",
        python_callable=twitter_etl,
        doc_md="""
        ## Получение данных из Twitter
        
        Эта задача получает данные из Twitter API и сохраняет их.
        """,
    )

    # --- ОПРЕДЕЛЕНИЕ ЗАВИСИМОСТЕЙ ---
    get_parameters >> twitter_task
     