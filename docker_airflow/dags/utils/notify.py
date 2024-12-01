def notify_on_failure(context):
    from utils.send_tg_message import send_message

    error_message =f'Ошибка в задаче {context["task_instance"].task_id} в DAG {context["dag"].dag_id}: {context["exception"]}'
    send_message(error_message)