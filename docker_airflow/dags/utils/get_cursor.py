def get_cursor(conn_id):
   """
   Функция get_cursor возвращает курсор для выполнения SQL-запросов на базе данных PostgreSQL.

   Аргументы:
   conn_id -- идентификатор подключения к базе данных.

   Процесс:
   1. Импортируем класс PostgresHook из модуля airflow.providers.postgres.hooks.postgres.
   2. Создаем экземпляр класса PostgresHook с указанным идентификатором подключения.
   3. Получаем соединение с базой данных, используя метод get_conn().
   4. Возвращаем курсор для выполнения SQL-запросов.

   Пример использования:
   >>> get_cursor('my_conn_id')
   """


   from airflow.providers.postgres.hooks.postgres import PostgresHook


   hook = PostgresHook(postgres_conn_id=conn_id)
   connection = hook.get_conn()
   
   return connection.cursor()
  