import telepot
from time import sleep

token = '971551324:AAGz8COn-WvxBWbbr_0N5bjeJVyIAAu487A'
TelegramBot = telepot.Bot(token)
#print(TelegramBot.getMe())
#print(TelegramBot.getUpdates())

i = 0
while True:
    i = 0
    msg = TelegramBot.getUpdates()
    #print(TelegramBot.getUpdates())
    print('checkeando bot status...')
    for element in msg:
        for key, value in element.items():
            print('key: ' + str(key))
            print('value: ' + str(value))
            if 'message' in key:
                chat_id = str(value['chat']['id'])
                print('mensaje ' + str(i) + ': ' + str(value['text']))
                i += 1
    TelegramBot.sendMessage(chat_id=chat_id, text=str(value['text']))
    sleep(5)
