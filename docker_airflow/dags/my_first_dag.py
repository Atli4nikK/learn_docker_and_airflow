from datetime import datetime, timedelta
import textwrap

# The DAG object; we'll need this to instantiate a DAG
from airflow.models.dag import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator

from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow.utils.dates import days_ago
from airflow.models.dag  import DagContext

# [END import_module]

def dev2prod_data(**context):
    ui_run_id = context['dag_run'].run_id#Забираем данные запуска дага из контекста
    #Работа с источником
    #Получаем соединение с базой dev
    pg_hook = PostgresHook(postgres_conn_id='Conn1') #Коннект из с вебинтерфейса аирфлоу
    connection = pg_hook.get_conn() # Получаем коннект с помощью Хука
    cursor = connection.cursor() # Получаем курсор постгрес

    #Получаем набор данных из dev
    sql = 'SELECT * FROM public.newtable;'
    cursor.execute(sql) # Получаем набор данных в курсор
    sources = cursor.fetchall() # Передаем все строки из запроса в кортеж(tuple)
    
    #Работа с таргетом
    #Получаем соединений с базой prod
    pg_hook_prod = PostgresHook(postgres_conn_id='Conn2')
    connection_prod = pg_hook_prod.get_conn() # Получаем коннект с помощью Хука
    cursor_prod = connection_prod.cursor() # Получаем курсор постгрес

    #Формируем запрос на вставку данных в прод
    sql_insert = 'TRUNCATE TABLE public.newtable;'
    for source in sources:
        sql_insert = sql_insert + 'INSERT INTO public.newtable (column1, run_id) VALUES('+ str(source)[1:-1] +');' # Собираем запрос на вставку
    sql_insert = sql_insert + 'commit;' # Коммитим вставленные строки
    cursor_prod.execute(sql_insert) # Вставляем данные

    #Логируем в таблицу log, связываем Даг и ID запуска
    sql_run_id = 'select max(run_id) from public.newtable;'
    cursor_prod.execute(sql_run_id)
    run_id = cursor_prod.fetchone() #Проучаем кастомный id запуска Дага
    sql_log = "insert into public.log(dag, run_id, ui_run_id)values('my_first_dag',"+ str(run_id)[1:-2] + ",'" + str(ui_run_id) +"');commit;"
    cursor_prod.execute(sql_log)#Привязываем id запуска к дагу в логах

    return sql_insert

def insert_data_dev():
    #Получаем соединение с базой данных дев для дальнейшей работы с ней
    pg_hook_dev = PostgresHook(postgres_conn_id='Conn1')
    connection_dev = pg_hook_dev.get_conn()
    cursor_dev = connection_dev.cursor()

    #Получаем кастомный id запуска дага
    select_run_id_last = 'select max(run_id) from public.newtable;'
    cursor_dev.execute(select_run_id_last)
    run_id_last = cursor_dev.fetchone()
    run_id_next = int(str(run_id_last)[1:-2]) + 1

    #Вставка пачкой по 10
    sql_insert_run_id = str()
    for i in range(10):
        sql_insert_run_id = sql_insert_run_id + 'insert into public.newtable(column1,run_id) values('+ str(666) + ',' + str(run_id_next) + ');' # Коммитим вставленные строки
    sql_insert_run_id = sql_insert_run_id + 'commit;'
    cursor_dev.execute(sql_insert_run_id) # Вставляем данные

    return run_id_next
 
# [START instantiate_dag]
with DAG(
    "my_first_dag",
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function, # or list of functions
        # 'on_success_callback': some_other_function, # or list of functions
        # 'on_retry_callback': another_function, # or list of functions
        # 'sla_miss_callback': yet_another_function, # or list of functions
        # 'on_skipped_callback': another_function, #or list of functions
        # 'trigger_rule': 'all_success'
    },
    # [END default_args]
    description="A simple tutorial DAG",
    schedule=timedelta(days=1),
    start_date=days_ago(2),
    catchup=False,
    tags=["example"],
) as dag:
    # [END instantiate_dag]
    # t1, t2 and t3 are examples of tasks created by instantiating operators
    # [START basic_task]
    bash = BashOperator(
        task_id="print_date",
        bash_command="date",
    )
    
    insert_select_newtable_1 = SQLExecuteQueryOperator(
        task_id="insert_select_newtable_1",
        conn_id = "Conn1",
        sql = "INSERT INTO public.newtable_1 SELECT * FROM public.newtable;",
        autocommit = True,
    )

    insert_data_dev = PythonOperator(
        task_id="insert_data_dev",
        python_callable=insert_data_dev
    )

    # Питон оператор для запуска питон функции
    migration_prod = PythonOperator(
        task_id="migration_prod",
        python_callable=dev2prod_data
    )
    # [END basic_task]
    
    bash >> insert_data_dev >> [insert_select_newtable_1, migration_prod]

# [END tutorial]