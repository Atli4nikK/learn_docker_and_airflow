# Learn docker and airflow and python

_Просто учу airflow, docker и python_

Архитектуру рисовал после создания первого DAG. Поэтому она пока не соотвествует тому, что сейчас реализовано.

![Архитектура](https://github.com/Atli4nikK/learn_docker_and_airflow/blob/master/architecture.jpg)

Источники, один из них база DEV, созданная нами, будет еще забор твиттов каких-нибудь и любые другие источники.
Информация из источников будет складывать в схему сырых данных STG.
Из STG данные будут как-то преобразовываться и переливаться в схему CORE.
На основании данных из CORE будут рассчитываться DM (витрины данных). 
По идее, некоторые витрины могут рассчитываться и на STG данных. 
Зависит от того, что надо умным людям для анализа. Умные люди анализируют данные используя всякие свои инструменты BI и др.

Всё развернуто на Windows 11. Но учитывая использование docker, то похер где это развернуто

Развернуто 2 базы Postgres (DEV, PROD) названия ничего не говорят о их предназначении, вообще они должны называться OLTP и DWH соответсвенно.
Что бы развернуть, надо установить docker на ПК и запустить https://www.docker.com/products/docker-desktop/
После надо перейти в директории наших баз данных и запустить docker-compose up

Airflow разворачивал по их документации https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

Для работы с подключениями к БД, надо создать Connections

Я создавал через UI.

DEV
- Connection ID: Conn1; Connection Type: Postgres; Host: host.docker.internal (Это локал хост докера); Database: dev; Login: admin; Password: admin; Port: 5432

PROD
- Connection ID: Conn2; Connection Type: Postgres; Host: host.docker.internal (Это локал хост докера); Database: prod; Login: admin2; Password: admin2; Port: 5433

DAG my_first_dag имеет 4 task:
- print_date просто с помощью BashOperator выводит дату в консоль. Моя первая task.
- insert_data_dev используя PythonOperator вставляет в таблицу public.newtable (DEV) пачками по 10 строк со значением 666 и кастомным run_id (каждый следующий run_id это прошлый + 1)
- insert_select_newtable_1 еще одна бесполезная task, которая с помощью SQLExecuteQueryOperator переливает данные из public.newtable в public.newtable_1 в рамках БД DEV
- migration_prod это самая полезная task, переливает данные между DEV и PROD, так же получает run_id (который отображается в UI) из контекста запуска DAG и логирует это в таблице public.log, связывая название DAG кастомный run_id и оригинальный run_id
