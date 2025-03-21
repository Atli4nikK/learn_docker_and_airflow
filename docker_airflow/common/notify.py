def notify_on_failure(context):
    from common.send_tg_message import send_message
    from common.get_cursor import get_cursor

    cursor_prod = get_cursor("Conn1")
    sql = f'select um.fail_proc({context["dag"].dag_id}, {context["dag_run"].run_id}); commit;'
    cursor_prod.execute(sql)
    error_message =f'Ошибка в задаче {context["task_instance"].task_id} в DAG {context["dag"].dag_id}: {context["exception"]}'
    send_message(error_message)

def notify_on_success(**context):
    from common.send_tg_message import send_message
    from common.get_cursor import get_cursor

    cursor_prod = get_cursor("Conn1")
    sql = f"select um.success_proc('{context['dag'].dag_id}', {context['dag_run'].run_id}); commit;"
    cursor_prod.execute(sql)
    success_message =f'Задача успешно выполнена'
    send_message(success_message)