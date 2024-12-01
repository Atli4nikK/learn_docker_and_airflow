def send_message(message):

    from utils.config import tg_bot_token, chat_id
    import requests

    url = f'https://api.telegram.org/bot{tg_bot_token}/sendMessage'

    params = {
        'chat_id' : chat_id,
        'text' : message,
    }

    response = requests.get(url, params)

    if response.status_code == 200:
        print('Message sent')
    else:
        print('Message not sent')