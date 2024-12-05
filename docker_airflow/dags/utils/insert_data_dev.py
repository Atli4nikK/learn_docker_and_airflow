def insert_data_dev(**context):
    """
    Функция insert_data_dev вставляет данные в базу данных.

    Процесс:
    1. Получаем кастомный id запуска дага.
    2. Формируем запрос на вставку данных в базу данных.
    3. Вставляем данные в базу данных.
    4. Возвращаем следующий id запуска.
    """


    from utils.get_cursor import get_cursor


    # Получаем кастомный id запуска дага
    select_run_id_last = 'select max(run_id) from public.newtable;'
    cursor_dev = get_cursor("Conn1")
    cursor_dev.execute(select_run_id_last)
    run_id = context['dag_run'].run_id

    # Вставка пачкой по 10
    sql_insert_run_id = str()
    for i in range(10):
        sql_insert_run_id = sql_insert_run_id + 'insert into public.newtable(column1,run_id) values(' + str(666) + ',' + str(run_id) + ');'  # Коммитим вставленные строки
    sql_insert_run_id = sql_insert_run_id + 'commit;'
    cursor_dev.execute(sql_insert_run_id)  # Вставляем данные

    return run_id
