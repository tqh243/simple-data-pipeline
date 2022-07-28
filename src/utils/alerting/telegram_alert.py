import os

import telegram

ADMIN_TOKEN = os.getenv('TELEGRAM_ADMIN_TOKEN')

def send_message(
    message: str,
    chat_id: str,
    parse_mode='markdown'
):
    bot = telegram.Bot(token=ADMIN_TOKEN)
    if len(message) > 1000:
        message = message[:1000]

    message = message.replace('<', '[')\
                     .replace('>', ']')

    if parse_mode == 'html':
        bot.sendMessage(chat_id=chat_id, text=message, parse_mode=telegram.ParseMode.HTML)
    else:
        bot.sendMessage(chat_id=chat_id, text=message, parse_mode=telegram.ParseMode.MARKDOWN)
