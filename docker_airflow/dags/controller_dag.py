"""
DAG-контроллер для динамического запуска других DAG
"""
from datetime import datetime, timedelta

# Импорты Airflow
from airflow.models.dag import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
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
# def get_dag():
#     """
#     Получает список DAG-ов для запуска из БД
    
#     Returns:
#         tuple: Кортеж из трех списков (dags, runs, parameters)
#     """
#     cursor_prod = get_cursor("Conn1")
#     sql_prod = """SELECT run_id, dag, params FROM um.loading where flag = '0';"""
#     cursor_prod.execute(sql_prod)
#     data_prod = cursor_prod.fetchall()

#     dags = []
#     runs = []
#     parameters = []

#     for data in data_prod:
#         dag_id = data[1]
#         run_id = data[0]
#         params_dag = data[2]
#         # Store multiple dag_id, run_id pairs in the dictionary
#         dags.append(dag_id)
#         runs.append(run_id)
#         parameters.append(params_dag)

#     return dags, runs, parameters

# Способ через expand
def get_dag_params():
    """Возвращает список параметров для запуска DAG'ов"""
    cursor_prod = get_cursor("Conn1")
    sql_prod = """SELECT run_id, dag, params FROM um.loading where flag = '0';"""
    cursor_prod.execute(sql_prod)
    return cursor_prod.fetchall()

# --- ОПРЕДЕЛЕНИЕ DAG ---
with DAG(
    DAG_ID,
    # Аргументы по умолчанию для всех задач
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 4,
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
    reg_task = PostgresOperator(
        task_id="reg_task",
        sql="select um.dag_reg();",
        autocommit = True,
        postgres_conn_id="Conn1",
        doc_md="""
        ## Выполнение процедуры регистрации
        
        Запускает хранимую процедуру dag_reg() в схеме um.
        """,
    )

    # Список задач для запуска DAG-ов
    # trigger_tasks = []

    # # Динамическое создание задач для запуска DAG-ов
    # for i in range(len(dags)):
    #     trigger_task = TriggerDagRunOperator(
    #         task_id=f"trigger_{dags[i]}",
    #         trigger_dag_id=dags[i],
    #         trigger_run_id=runs[i],
    #         conf=parameters[i],
    #         doc_md=f"""
    #         ## Запуск DAG {dags[i]}
            
    #         Триггерит запуск DAG {dags[i]} с run_id={runs[i]}.
    #         """,
    #     )
    #     trigger_tasks.append(trigger_task)

    # Способ через expand
    # Создаем базовый оператор с partial
    trigger_base = TriggerDagRunOperator.partial(
        task_id="trigger_dag",
    ).expand_kwargs( # Расширяем оператор с параметрами из БД
        [ # Формируем список словарей, сначала задаем структуру словаря, потом перебираем параметры
            {
                "trigger_dag_id": item[1],
                "trigger_run_id": item[0],
                "conf": item[2]
            }
            for item in get_dag_params()
        ]
    )
    
    running_dag_state_task = PostgresOperator(
        task_id="running_dag_state_task",
        autocommit = True,
        postgres_conn_id="Conn1",
        sql="""
        update um.loading
        set flag = '3',
            start_date = now()
        where flag = '0'
        """,
        doc_md="""
        ## Изменение статуса рега в таблице um.loading
        
        Изменение статуса на 3 (статус running)
        """,
    )

    # Определяем порядок выполнения задач
    reg_task >> trigger_base >> running_dag_state_task
