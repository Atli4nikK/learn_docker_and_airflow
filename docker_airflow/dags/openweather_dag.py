"""
DAG для получения данных о погоде с OpenWeather API
"""
from datetime import datetime, timedelta

# Импорты Airflow
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Пользовательские модули
from utils.notify import notify_on_failure, notify_on_success
from utils.openweather_etl import get_weather

# --- КОНФИГУРАЦИЯ DAG ---
DAG_ID = "openweather_dag"
DAG_DESCRIPTION = "Получение и обработка данных о погоде"
DAG_SCHEDULE = None
DAG_CATCHUP = False
DAG_TAGS = ["example"]

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
        "retries": 0,
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

    # Python-задача для получения данных о погоде
    python_task = PythonOperator(
        task_id="python_task",
        python_callable=get_weather,
        doc_md="""
        ## Получение данных о погоде
        
        Эта задача получает данные о погоде из OpenWeather API и сохраняет их.
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
    get_parameters >> python_task >> success
     