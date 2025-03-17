def upload_to_s3(**kwargs):
    """
    Функция upload_to_s3 загружает изображения из локального каталога в S3.

    Аргументы:
    keyword -- ключевое слово для поиска изображений.
    s3_dir -- директория в S3 для загрузки изображений.
    bucket_name -- имя бакета S3.

    Процесс:
    1. Импортируем библиотеку airflow.providers.amazon.aws.hooks.s3.
    2. Создаем объект S3Hook для взаимодействия с S3.
    3. Проходим по всем файлам в локальном каталоге.
    4. Загружаем каждый файл в S3 в указанную директорию и бакет.

    """
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    import os


    keyword = kwargs['keyword']
    s3_dir = kwargs['s3_dir']
    bucket_name = kwargs['bucket_name']

    local_dir = f'/opt/airflow/dags/esic/data/{keyword}'

    s3_hook = S3Hook(aws_conn_id='s3_conn_id')
    upload_images = 0
    for root, dirs, files in os.walk(local_dir):
        for file in files:
            local_path = os.path.join(root, file)
            s3_path = os.path.join(s3_dir, file)
            
            s3_hook.load_file(
                filename=local_path,
                key=s3_path,
                bucket_name=bucket_name,
                replace=True
            )
            upload_images = upload_images + 1
    
    print(f'В s3 загружено изображений: {upload_images}')