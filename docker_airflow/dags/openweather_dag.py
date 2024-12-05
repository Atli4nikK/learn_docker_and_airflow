from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from datetime import timedelta

from utils.notify import notify_on_failure, notify_on_success
from utils.openweather_etl import get_weather

defautl_args = {
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
    #'on_success_callback': notify_on_success, # or list of functions
    # 'on_retry_callback': another_function, # or list of functions
    # 'sla_miss_callback': yet_another_function, # or list of functions
    # 'on_skipped_callback': another_function, #or list of functions
    # 'trigger_rule': 'all_success'
}
def get_params(**context):
    params = context['dag_run'].conf

with DAG(
    'openweather_dag',
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args=defautl_args,
    # [END default_args]
    description="WeatherDag",
    schedule=None,
    start_date=days_ago(2),
    catchup=False,
    tags=["example"]
) as dag:
    
    get_parameters =PythonOperator(
        task_id="get_parameters",
        python_callable=get_params,
    )

    python_task = PythonOperator(
        task_id="python_task",
        python_callable=get_weather,
        # op_kwargs: Optional[Dict] = None,
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
    )

    success = PythonOperator(
        task_id="success",
        python_callable=notify_on_success,
    )

    get_parameters>>python_task >> success
     