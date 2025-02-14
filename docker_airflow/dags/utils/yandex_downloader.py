def yandex_downloader(**context):
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    from selenium.webdriver.common.keys import Keys
    import time
    import requests
    import os
    from requests.exceptions import RequestException
    from PIL import Image
    from io import BytesIO
    
    
    # Настройки  
    QUERY = context['keyword']              
    NUM_IMAGES = context.get('num_images')
    SAVE_FOLDER = f'/opt/airflow/dags/data/{QUERY}'  # Папка для сохранения
    MIN_IMAGE_SIZE = 50  # Минимальный размер изображения (ширина или высота)

    # Функция для загрузки обработанных URL
    def load_downloaded_urls(file_path='/opt/airflow/dags/data/downloaded_urls.txt'):
        downloaded_urls = set()
        try:
            with open(file_path, 'r') as f:
                for line in f:
                    downloaded_urls.add(line.strip())
        except FileNotFoundError:
            pass
        return downloaded_urls

    # Загружаем уже обработанные URL
    downloaded_urls = load_downloaded_urls()

    # Создаем папку, если её нет
    if not os.path.exists(SAVE_FOLDER):
        os.makedirs(SAVE_FOLDER)
        print(f"Папка '{SAVE_FOLDER}' создана.")
    else:
        print(f"Папка '{SAVE_FOLDER}' уже существует.")

    # Укажите путь к chromedriver
    # driver_path = "C:/Users/koldyrkaev/Desktop/python/airflow1/car_checker/chromedriver-win64/chromedriver.exe"

    # Создаем объект Service
    # service = Service(executable_path=driver_path)

    chrome_options = webdriver.ChromeOptions()
    # chrome_options.add_argument("--headless")  # Включаем headless-режим
    # chrome_options.add_argument("--disable-gpu")  # Отключаем GPU (рекомендуется для headless)

    # Инициализация драйвера
    # driver = webdriver.Chrome(service=service, options=chrome_options)
    driver = webdriver.Remote(command_executor='http://selenium-chrome:4444',  # Имя сервиса из docker-compose
                                options=chrome_options)
    print("Браузер запущен.")

    try:
        # Открываем Яндекс.Картинки
        driver.get(f"https://yandex.ru/images/search?text={QUERY}")
        print(f"Открыта страница с запросом: {QUERY}")

        # Ожидание загрузки страницы
        time.sleep(3)
        print("Страница загружена.")

        # Сбор ссылок на изображения
        urls = set()  # Используем set, чтобы избежать дубликатов
        scroll_attempts = 0
        max_scroll_attempts = 20  # Максимальное количество попыток прокрутки

        while True:
            # Прокрутка страницы вниз
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            print(f"Прокрутка страницы {scroll_attempts + 1}...")
            time.sleep(5)  # Ожидание загрузки новых изображений

            # Извлечение URL изображений
            images = driver.find_elements(By.CSS_SELECTOR, ".ImagesContentImage-Image")
            for img in images:
                src = img.get_attribute("src")
                if src and (src.startswith("http") or src.startswith("https")) and src not in downloaded_urls:
                    urls.add(src)

            scroll_attempts += 1
            print(f"Всего собрано {len(urls)} URL изображений.")

            # Нажатие на кнопку "Показать ещё", если она есть
            try:
                show_more_button = driver.find_element(By.CSS_SELECTOR, ".FetchListButton-Button")
                if show_more_button:
                    show_more_button.click()
                    print("Нажата кнопка 'Показать ещё'.")
                    time.sleep(2)  # Ожидание загрузки новых изображений
            except Exception as e:
                print(f"!!! Кнопка 'Показать ещё' не найдена !!!")
                break

        print(f"Собрано {len(urls)} URL изображений.")

        # Скачивание изображений
        downloaded_images = 0
        for i, url in enumerate(list(urls)):
            if downloaded_images >= NUM_IMAGES:
                break

            try:
                print(f"Попытка скачать изображение {downloaded_images + 1} из {url}...")
                response = requests.get(url, timeout=10)
                response.raise_for_status()  # Проверка на ошибки HTTP

                # Проверка размера изображения
                img = Image.open(BytesIO(response.content))
                width, height = img.size
                if width < MIN_IMAGE_SIZE or height < MIN_IMAGE_SIZE:
                    print(f"Изображение {downloaded_images + 1} слишком маленькое ({width}x{height}), пропускаем.")
                    continue

                # Сохранение изображения
                with open(f"{SAVE_FOLDER}/image_{str(int(time.time() * 1000))}_{downloaded_images + 1}.jpg", "wb") as file:
                    file.write(response.content)
                print(f"Изображение {downloaded_images + 1} успешно скачано.")
                
                downloaded_images += 1

                with open('/opt/airflow/dags/data/downloaded_urls.txt', 'a') as f:
                    f.write(url + '\n')
                    downloaded_urls.add(url)

            except RequestException as e:
                print(f"Ошибка при скачивании изображения {downloaded_images + 1}: {e}")
            except Exception as e:
                print(f"Ошибка при обработке изображения {downloaded_images + 1}: {e}")

        print(f"Скачано {downloaded_images}.")

    finally:
        # Закрытие браузера
        driver.quit()
        time.sleep(10)
        print("Браузер закрыт.")