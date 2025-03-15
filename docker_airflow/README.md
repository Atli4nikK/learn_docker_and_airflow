# Организация кода Airflow по проектам

## Обзор решения

Данное решение позволяет:
1. Разделить код DAG-ов по проектам (отдельным репозиториям)
2. Деплоить все DAG-и в один инстанс Airflow
3. Использовать общие утилиты и библиотеки между проектами

## Структура директорий

```
airflow/
├── dags/              # Основная директория DAG-ов
│   ├── project1/      # DAG-и проекта 1 (монтируется из репозитория проекта 1)
│   ├── project2/      # DAG-и проекта 2 (монтируется из репозитория проекта 2)
│   └── project3/      # DAG-и проекта 3 (монтируется из репозитория проекта 3)
├── logs/              # Логи Airflow
├── plugins/           # Плагины Airflow
├── config/            # Конфигурация Airflow
└── common/            # Общие утилиты для всех проектов
    ├── __init__.py
    └── utils.py       # Общие функции и утилиты
```

## Настройка

### 1. Структура репозиториев

Каждый проект должен иметь следующую структуру:

```
project-repo/
├── dags/              # DAG-файлы проекта
│   ├── __init__.py
│   └── your_dag.py    # Ваши DAG-файлы
└── README.md
```

### 2. Настройка docker-compose.yaml

В файле `docker-compose.yaml` необходимо указать пути к репозиториям проектов:

```yaml
volumes:
  - ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags
  # Монтирование дополнительных репозиториев с проектами в подпапки dags
  - /path/to/project1/dags:/opt/airflow/dags/project1
  - /path/to/project2/dags:/opt/airflow/dags/project2
  - /path/to/project3/dags:/opt/airflow/dags/project3
  - ${AIRFLOW_PROJ_DIR:-.}/logs:/opt/airflow/logs
  - ${AIRFLOW_PROJ_DIR:-.}/config:/opt/airflow/config
  - ${AIRFLOW_PROJ_DIR:-.}/plugins:/opt/airflow/plugins
  # Общие библиотеки и утилиты для всех проектов
  - ${AIRFLOW_PROJ_DIR:-.}/common:/opt/airflow/common
```

Замените `/path/to/projectX` на реальные пути к вашим репозиториям.

### 3. Использование общих утилит

В каждом DAG-файле вы можете импортировать общие утилиты:

```python
import sys
sys.path.append('/opt/airflow')
from common.utils import get_default_args, get_project_path, get_project_config
```

### 4. Именование DAG-ов

Рекомендуется использовать префиксы с именем проекта для всех DAG-ов, чтобы избежать конфликтов имен:

```python
PROJECT_NAME = 'project1'
DAG_ID = f"{PROJECT_NAME}_example_dag"
```

### 5. Тегирование DAG-ов

Используйте теги для группировки DAG-ов по проектам:

```python
with DAG(
    DAG_ID,
    default_args=get_default_args(owner=PROJECT_NAME),
    schedule_interval='@daily',
    catchup=False,
    tags=[PROJECT_NAME]  # Добавляем тег проекта
) as dag:
    # ...
```

## Обновление кода

Для обновления кода проектов:

1. Внесите изменения в соответствующий репозиторий проекта
2. При запуске Airflow новые DAG-файлы будут автоматически загружены

Для проектов, которые требуют более сложного управления версиями или CI/CD, рекомендуется настроить отдельный пайплайн, который будет:
1. Клонировать репозиторий в определенную директорию
2. Обновлять эту директорию при изменениях
3. Эта директория монтируется в Docker контейнер Airflow 