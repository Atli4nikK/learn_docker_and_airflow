# ESIC

Система интеграции и обработки данных для ETL процессов с использованием Airflow.

## Описание

ESIC (ETL System for Integration and Control) - это набор DAG-ов Airflow для ETL процессов (извлечение, преобразование, загрузка данных). Система обеспечивает интеграцию данных из различных источников, их трансформацию и загрузку в хранилище данных.

## Структура проекта

```
ESIC/
├── dags/                  # DAG-файлы Airflow
│   ├── esic_example.py    # Пример DAG
│   └── ...                # Другие DAG-файлы
└── ...                    # Дополнительные файлы и директории
```

## Установка и настройка

Данный репозиторий предназначен для использования с инстансом Airflow, где он монтируется как директория с DAG-файлами.

1. Клонировать репозиторий:
   ```bash
   git clone https://github.com/Atli4nikK/ESIC.git
   ```

2. Обеспечить подключение репозитория к Airflow через docker-compose.yaml:
   ```yaml
   volumes:
     - /path/to/ESIC/dags:/opt/airflow/dags/esic
   ```

## Использование

После подключения репозитория к Airflow, DAG-и будут доступны в веб-интерфейсе Airflow с префиксом "esic_".

## Требования

- Apache Airflow 2.0+
- Python 3.8+
- Доступ к общим библиотекам, определенным в директории common

## Разработка

При разработке новых DAG-ов для ESIC следуйте этим рекомендациям:

1. Используйте префикс "esic_" для имен DAG-ов
2. Добавляйте тег "esic" для всех DAG-ов
3. Используйте общие утилиты из директории common через правильный импорт:
   ```python
   import sys
   sys.path.append('/opt/airflow')
   from common.utils import get_default_args
   ``` 