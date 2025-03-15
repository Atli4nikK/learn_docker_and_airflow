"""
DAG для базовой демонстрации Airflow
"""
import textwrap
from datetime import datetime, timedelta

# Импорты Airflow
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Пользовательские модули
from utils.dev2prod_data import dev2prod_data
from utils.insert_data_dev import insert_data_dev
from utils.notify import notify_on_failure, notify_on_success

# --- КОНФИГУРАЦИЯ DAG ---
DAG_ID = "my_first_dag"
DAG_DESCRIPTION = "A simple tutorial DAG"
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

    # Выполнение Bash-команды
    bash = BashOperator(
        task_id="print_date",
        bash_command="date",
        doc_md="""
        ## Вывод текущей даты
        
        Эта задача выводит текущую дату с помощью команды Bash.
        """,
    )
    
    # SQL-запрос для вставки данных
    insert_select_newtable_1 = SQLExecuteQueryOperator(
        task_id="insert_select_newtable_1",
        conn_id="Conn1",
        sql="INSERT INTO public.newtable_1 SELECT * FROM public.newtable;",
        autocommit=True,
        doc_md="""
        ## Вставка данных в таблицу
        
        Копирует данные из таблицы public.newtable в public.newtable_1.
        """,
    )

    # Python-задача для вставки данных
    insert_data_dev = PythonOperator(
        task_id="insert_data_dev",
        python_callable=insert_data_dev,
        doc_md="""
        ## Вставка данных в DEV
        
        Вставляет тестовые данные в DEV-среду.
        """,
    )

    # Python-задача для миграции данных
    migration_prod = PythonOperator(
        task_id="migration_prod",
        python_callable=dev2prod_data,
        doc_md="""
        ## Миграция данных в PROD
        
        Мигрирует данные из DEV в PROD-среду.
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
    get_parameters >> bash >> insert_data_dev >> [insert_select_newtable_1, migration_prod] >> success