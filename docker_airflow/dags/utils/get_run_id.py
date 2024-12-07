def get_run_id():

    from utils.get_cursor import get_cursor

    cursor_prod = get_cursor("Conn2")
    sql_run_id = 'select max(run_id) from public.newtable;'
    cursor_prod.execute(sql_run_id)
    run_id = cursor_prod.fetchone()

    return str(run_id)[1:-2]
