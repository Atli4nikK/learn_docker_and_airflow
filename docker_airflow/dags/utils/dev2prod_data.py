def dev2prod_data(**context):
    """
    Функция dev2prod_data копирует данные из базы данных dev в базу данных prod.

    Аргументы:
    context -- контекст выполнения дага.

    Процесс:
    1. Получаем данные запуска дага из контекста.
    2. Получаем курсор для работы с базой данных dev.
    3. Получаем набор данных из базы данных dev.
    4. Получаем курсор для работы с базой данных prod.
    5. Формируем запрос на вставку данных в prod.
    6. Вставляем данные в prod.
    """

    from common.get_cursor import get_cursor


    # Работа с источником
    cursor_dev = get_cursor("Conn1")

    sql = 'SELECT * FROM public.newtable;'
    cursor_dev.execute(sql)
    sources = cursor_dev.fetchall()

    # Работа с таргетом
    cursor_prod = get_cursor("Conn2")

    sql_insert = 'TRUNCATE TABLE public.newtable;'
    for source in sources:
        sql_insert = sql_insert + 'INSERT INTO public.newtable (column1, run_id) VALUES(' + str(source)[1:-1] + ');'
    sql_insert = sql_insert + 'commit;'
    cursor_prod.execute(sql_insert)

    # Надо изучить другой варинат
    # hook_dev = PostgresHook(postgres_conn_id='Conn1',schema='public')
    # data = hook_dev.get_pandas_df('''select * from newtable ''')

    # hook_prod = PostgresHook(postgres_conn_id='Conn2',schema='public')
    # hook_prod.insert_rows(table='newtable', rows=data)
        
    return sql_insert
