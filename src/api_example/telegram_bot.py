#!/usr/bin/python3

import telepot
from time import sleep

TOKEN = '971551324:AAGz8COn-WvxBWbbr_0N5bjeJVyIAAu487A'

def telegram_sendMessage(msg_telegram):
    TelegramBot = telepot.Bot(TOKEN)
    msg_counter = 0
    msg = TelegramBot.getUpdates()
    for element in msg:
        for key, value in element.items():
            if 'message' in key:
                chat_id = str(value['chat']['id']) # catchear si no existe chat_id
                print('mensaje ' + str(msg_counter) + ': ' + str(value['text']))
                msg_counter += 1
    TelegramBot.sendMessage(chat_id=chat_id, parse_mode = 'html', text='<b>==========================</b> ')
    TelegramBot.sendMessage(chat_id=chat_id, parse_mode = 'html', text='<b>Nuevo mensaje:</b> ' + str(msg_telegram))


TelegramBot = telepot.Bot(TOKEN)
#print(TelegramBot.getMe())
#print(TelegramBot.getUpdates())

i = 0
while True:
    i += 1
    telegram_sendMessage('probando mensaje ' + str(i))
    sleep(5)
