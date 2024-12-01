from datetime import datetime, timedelta
import textwrap

# The DAG object; we'll need this to instantiate a DAG
from airflow.models.dag import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models.dag  import DagContext

from utils.dev2prod_data import dev2prod_data
from utils.insert_data_dev import insert_data_dev
from utils.notify import notify_on_failure
# [END import_module]
 
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
        'on_failure_callback': notify_on_failure, # or list of functions
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