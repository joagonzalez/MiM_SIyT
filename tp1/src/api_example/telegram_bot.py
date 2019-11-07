import telepot
from time import sleep

token = '971551324:AAGz8COn-WvxBWbbr_0N5bjeJVyIAAu487A'
TelegramBot = telepot.Bot(token)
# print(TelegramBot.getMe())
print(TelegramBot.getUpdates())

i = 0
while True:
    i = 0
    #print(TelegramBot.getUpdates())
    print('checkeando bot status...')
    for element in TelegramBot.getUpdates():
        for key, value in element.items():
            #print('key: ' + str(key))
            #print('value: ' + str(value))
            if 'message' in key:
                print('mensaje ' + str(i) + ': ' + str(value['text']))
                i += 1
    sleep(5)