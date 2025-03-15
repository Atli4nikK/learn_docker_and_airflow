"""
DAG для регистрации и запуска SQL процедур
"""
from datetime import datetime, timedelta

# Импорты Airflow
from airflow.models.dag import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.dates import days_ago

# Пользовательские модули
from utils.notify import notify_on_failure

# --- КОНФИГУРАЦИЯ DAG ---
DAG_ID = "reg_dag"
DAG_DESCRIPTION = "Регистрация и запуск SQL процедур"
DAG_SCHEDULE = '0 */1 * * *'  # Каждый час
DAG_CATCHUP = False
DAG_TAGS = ["example"]

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
    
    # SQL-задача для запуска процедуры регистрации
    reg_task = SQLExecuteQueryOperator(
        task_id="reg_task",
        sql="select um.dag_reg();",
        conn_id="Conn1",
        doc_md="""
        ## Выполнение процедуры регистрации
        
        Запускает хранимую процедуру dag_reg() в схеме um.
        """,
    )

