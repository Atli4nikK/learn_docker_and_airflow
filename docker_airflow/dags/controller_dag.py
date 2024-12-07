from airflow.models.dag import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from utils.get_run_id import get_run_id
from utils.notify import notify_on_failure
from utils.get_cursor import get_cursor



default_args = {
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
}

def get_dag():
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

dags, runs, parameters = get_dag()

with DAG(
    "controller_dag",
    default_args=default_args,
    description="Controller DAG",
    catchup=False,
    schedule='5 */1 * * *',
    start_date=days_ago(2),
    tags=["example"],
) as dag:
    
    for i in range(len(dags)):
        TriggerDagRunOperator(
            task_id="trigger_" + dags[i],
            trigger_dag_id=dags[i],
            trigger_run_id=runs[i],
            conf=parameters[i],
        )

