"""
DAG-контроллер для динамического запуска других DAG
"""
from datetime import datetime, timedelta

# Импорты Airflow
from airflow.models.dag import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.dates import days_ago

# Пользовательские модули
from common.notify import notify_on_failure
from common.get_cursor import get_cursor

# --- КОНФИГУРАЦИЯ DAG ---
DAG_ID = "controller_dag"
DAG_DESCRIPTION = "Контроллер для динамического запуска DAG"
DAG_SCHEDULE = '5 */1 * * *'  # 5 минут каждого часа
DAG_CATCHUP = False
DAG_TAGS = ["koldyrkaevs"]

# --- ОПРЕДЕЛЕНИЕ ФУНКЦИЙ ---
def get_dag():
    """
    Получает список DAG-ов для запуска из БД
    
    Returns:
        tuple: Кортеж из трех списков (dags, runs, parameters)
    """
    cursor_prod = get_cursor("Conn1")
    sql_prod = """SELECT run_id, dag, params FROM um.loading where flag = '0';"""
    cursor_prod.execute(sql_prod)
    data_prod = cursor_prod.fetchall()

    dags = []
    runs = []
    parameters = []

    for data in data_prod:
        dag_id = data[1]
        run_id = data[0]
        params_dag = data[2]
        # Store multiple dag_id, run_id pairs in the dictionary
        dags.append(dag_id)
        runs.append(run_id)
        parameters.append(params_dag)

    return dags, runs, parameters

# Получаем списки DAG-ов для запуска
dags, runs, parameters = get_dag()

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
    
    # Динамическое создание задач для запуска DAG-ов
    for i in range(len(dags)):
        trigger_task = TriggerDagRunOperator(
            task_id=f"trigger_{dags[i]}",
            trigger_dag_id=dags[i],
            trigger_run_id=runs[i],
            conf=parameters[i],
            doc_md=f"""
            ## Запуск DAG {dags[i]}
            
            Триггерит запуск DAG {dags[i]} с run_id={runs[i]}.
            """,
        )

