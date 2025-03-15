# Организация кода Airflow по проектам

## Реализованное решение

В данном решении:
1. Код DAG-ов разделен по репозиториям (autotrend и ESIC)
2. Все DAG-и деплоятся в один инстанс Airflow
3. Общие утилиты и библиотеки размещены в директории common

## Структура проекта

```
C:/Users/koldyrkaev/Desktop/python/airflow1/learn_docker_and_airflow/docker_airflow/
├── dags/                 # Стандартная директория DAG-ов Airflow
├── common/               # Общие утилиты для всех проектов
│   ├── __init__.py
│   └── utils.py          # Общие функции и утилиты

C:/Users/koldyrkaev/Desktop/GIT/autotrend/
└── dags/                 # DAG-и проекта autotrend
    ├── __init__.py
    └── autotrend_example.py

C:/Users/koldyrkaev/Desktop/GIT/ESIC/
└── dags/                 # DAG-и проекта ESIC
    ├── __init__.py
    └── esic_example.py
```

## Как это работает

1. В docker-compose.yaml настроено монтирование директорий с DAG-ами из разных репозиториев:
   ```yaml
   volumes:
     - ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags
     # Монтирование ваших репозиториев
     - C:/Users/koldyrkaev/Desktop/GIT/autotrend/dags:/opt/airflow/dags/autotrend
     - C:/Users/koldyrkaev/Desktop/GIT/ESIC/dags:/opt/airflow/dags/esic
     - ${AIRFLOW_PROJ_DIR:-.}/common:/opt/airflow/common
   ```

2. В каждом DAG-файле используются общие утилиты из директории common:
   ```python
   import sys
   sys.path.append('/opt/airflow')
   from common.utils import get_default_args, get_project_path, get_project_config
   ```

3. DAG-и идентифицируются по проектам с помощью префиксов и тегов:
   ```python
   PROJECT_NAME = 'autotrend'  # или 'esic'
   DAG_ID = f"{PROJECT_NAME}_example_dag"
   ```

## Использование

1. **Разработка DAG-ов для проекта autotrend**:
   - Работайте в репозитории `C:/Users/koldyrkaev/Desktop/GIT/autotrend`
   - Создавайте DAG-файлы в директории `dags/`
   - Используйте префикс `autotrend_` для имен DAG-ов
   - Добавляйте тег `autotrend` для всех DAG-ов

2. **Разработка DAG-ов для проекта ESIC**:
   - Работайте в репозитории `C:/Users/koldyrkaev/Desktop/GIT/ESIC`
   - Создавайте DAG-файлы в директории `dags/`
   - Используйте префикс `esic_` для имен DAG-ов
   - Добавляйте тег `esic` для всех DAG-ов

3. **Разработка общих утилит**:
   - Работайте в директории `C:/Users/koldyrkaev/Desktop/python/airflow1/learn_docker_and_airflow/docker_airflow/common/`
   - Общие утилиты будут доступны для всех проектов

## Перезапуск Airflow

После внесения изменений в код DAG-ов или общие утилиты, перезапустите Airflow:

```bash
cd C:/Users/koldyrkaev/Desktop/python/airflow1/learn_docker_and_airflow/docker_airflow
docker-compose down
docker-compose up -d
```

## Преимущества подхода

1. **Изоляция кода**: Каждый проект разрабатывается в собственном репозитории
2. **Единый Airflow**: Все DAG-и выполняются в одном инстансе Airflow
3. **Общие утилиты**: Код, который используется в нескольких проектах, вынесен в общую директорию
4. **Простота обновления**: Изменения в одном проекте не влияют на другие проекты 