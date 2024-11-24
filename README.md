# Learn Docker, Airflow, and Python

_Просто учу Airflow, Docker и Python._

Архитектуру рисовал после создания первого DAG. Поэтому она пока не соответствует тому, что сейчас реализовано.

![Архитектура](https://github.com/Atli4nikK/learn_docker_and_airflow/blob/master/architecture.jpg)

## Источники данных

Источники данных включают:
- **DEV** — база данных, созданная нами.
- **Twitter**
- В будущем будут и другие источники данных.

Информация из источников будет складываться в схему сырых данных **STG**. Затем данные будут преобразовываться и переливаться в схему **CORE**. На основании данных из **CORE** будут рассчитываться **DM** (витрины данных). Некоторые витрины могут рассчитываться и на данных из **STG**, в зависимости от потребностей умных людей, использующих BI инструменты и т.д.

## Окружение

Все развернуто на **Windows 11**, но благодаря использованию Docker, не имеет значения, на какой операционной системе это работает.

- Развернуто 2 базы данных **Postgres**: **DEV** и **PROD**. Их названия не отражают их назначение, они должны называться **OLTP** и **DWH** соответственно.
  
### Как развернуть

1. Установите Docker на ПК, скачав и установив [Docker Desktop](https://www.docker.com/products/docker-desktop/).
2. Перейдите в директорию с базами данных и запустите:

    ```bash
    docker-compose up
    ```

### Развертывание Airflow

Для развертывания **Airflow** использовалась официальная документация: [Docker Compose для Airflow](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html).

### Подключения к базам данных

Для работы с подключениями к базам данных нужно создать **Connections**.

Я создавал их через UI.

#### DEV
- **Connection ID**: Conn1
- **Connection Type**: Postgres
- **Host**: host.docker.internal (локальный хост Docker)
- **Database**: dev
- **Login**: admin
- **Password**: admin
- **Port**: 5432

#### PROD
- **Connection ID**: Conn2
- **Connection Type**: Postgres
- **Host**: host.docker.internal (локальный хост Docker)
- **Database**: prod
- **Login**: admin2
- **Password**: admin2
- **Port**: 5433

### Установка нестандартных библиотек

Для установки нестандартных библиотек был расширен образ, и на его основе пересобран **docker-compose**:

```bash
docker build . --tag extending_airflow:latest
```

### DAGs
#### my_first_dag
- **print_date** — использует BashOperator для вывода текущей даты в консоль. Моя первая task.
- **insert_data_dev** — использует PythonOperator для вставки данных в таблицу public.newtable (DEV) пакетами по 10 строк, где значение всех строк — 666 и кастомный run_id (каждый следующий run_id — это предыдущий + 1).
- **insert_select_newtable_1** — выполняет перенос данных из public.newtable в public.newtable_1 в рамках базы данных DEV с использованием SQLExecuteQueryOperator.
- **migration_prod** — переливает данные между DEV и PROD. Эта task также получает run_id (отображаемый в UI) из контекста выполнения DAG и логирует это в таблице public.log, связывая название DAG, кастомный run_id и оригинальный run_id.

#### twitter_dag
- **python_task** — собирает твитты и записывает их в PROD таблицу tweet.tweets.
