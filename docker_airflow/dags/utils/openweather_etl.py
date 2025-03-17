def get_weather(**context):
    from common.config import tg_bot_token, open_weather_token
    import requests
    import datetime as dt
    from common.send_tg_message import send_message


    '''
    Функция для получения погоды в городе
    '''
    
    code_to_smile = {
        "Clear": "Ясно \U00002600",
        "Clouds": "Облачно \U00002601",
        "Rain": "Дождь \U00002614",
        "Drizzle": "Дождь \U00002614",
        "Thunderstorm": "Гроза \U000026A1",
        "Snow": "Снег \U0001F328",
        "Mist": "Туман \U0001F32B",
    }
    
    try:
        r = requests.get(f'http://api.openweathermap.org/data/2.5/weather?q={context['dag_run'].conf.get("City")}&lang=ru&units=metric&appid={open_weather_token}&units=metric')
        data = r.json()
        #pprint(data)

        city = data['name']

        weather_description = data['weather'][0]['main']
        if weather_description in code_to_smile:
            wd = code_to_smile[weather_description]
        else:
            wd = 'Неизвестно'

        cur_weather = data['main']['temp']
        humidity = data['main']['humidity']
        pressure = data['main']['pressure']
        wind = data['wind']['speed']
        sunrise_timestamp = dt.datetime.fromtimestamp(data['sys']['sunrise'])
        sunset_timestamp = dt.datetime.fromtimestamp(data['sys']['sunset'])

        message = (f'***{dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}***\n'
                  f'City: {city}\n'
                  f'Weather: {wd}\n'
                  f'Temperature: {cur_weather} C\n'
                  f'Humidity: {humidity}\n'
                  f'Pressure: {pressure}\n'
                  f'Wind speed: {wind} m.s.\n'
                  f'Sunrise time: {sunrise_timestamp}\n'
                  f'Sunset time: {sunset_timestamp}')

        send_message(message)
    except:
        message = '\U00002620 Произошла ошибка \U00002620'
        send_message(message)
