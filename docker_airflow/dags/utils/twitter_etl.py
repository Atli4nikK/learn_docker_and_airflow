def twitter_etl():
    """
    Функция twitter_etl выполняет ETL-процесс для сбора и обработки данных о твитах пользователя @elonmusk.

    Процесс:
    1. Создаем объект для взаимодействия с Twitter API.
    3. Получаем твиты пользователя @elonmusk.
    4. Извлекаем информацию о пользовате, тексте твита, количестве лайков и ретвитов, дату создания твита.
    5. Добавляем словарь refined_tweet в список tweet_list.
    6. Создаем DataFrame из списка tweet_list.
    7. Сохраняем DataFrame в таблицу 'tweets' в базе данных.
    """


    from ntscraper import Nitter
    import pandas as pd

    from airflow.providers.postgres.hooks.postgres import PostgresHook

    from datetime import datetime


    # Инициализируем объект для взаимодействия с Twitter API
    scraper = Nitter(log_level=1, skip_instance_check=True)

    # Получаем твиты пользователя @elonmusk
    # 
    tweets = scraper.get_tweets("elonmusk", mode="user", number=100, instance="https://nitter.privacydev.net")

    tweet_list = []  # type: List[]
    for tweet in tweets["tweets"]:
        refined_tweet = {"user": tweet["user"]["username"],
                        "text": tweet["text"],
                        "favorite_count": tweet["stats"]["likes"],
                        "retweet_count": tweet["stats"]["retweets"],
                        "created_at": datetime.strptime(tweet["date"], "%b %d, %Y · %I:%M %p UTC").strftime("%Y-%m-%d %H:%M:%S+00")}  # Преобразование в timestamptz

        tweet_list.append(refined_tweet)

    df = pd.DataFrame(tweet_list)  # type: pd.DataFrame
    hook = PostgresHook(postgres_conn_id="Conn2")
    df.to_sql(name="tweets", schema="tweet", con=hook.get_sqlalchemy_engine(), if_exists="append", index=False)
    #df.to_csv("tweets.csv", index=False) #for testing
    
