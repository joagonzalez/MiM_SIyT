#!/usr/bin/python3

###################
#### LIBRARIES ####
###################

import requests
import json
import os
import sys
from time import sleep
from datetime import datetime
import argparse
import logging
import telepot

import pyarrow.parquet as pq
import numpy as np
import pandas as pd
import pyarrow as pa

from influxdb import DataFrameClient

###############
#### CONST ####
###############

API_TRANSPORTE_URL = 'https://apitransporte.buenosaires.gob.ar'
FILENAME = 'reports/json/bus_position_'
QRY_LENGTH = 1000
JSON_PATH = 'reports/json/'
PARQUET_PATH = 'reports/parquet/'
LINEAS = ['29A','41A','44A','57A','59A','63A','65A','67A','68A','80A','107A','113A','133A','152A','161A','168A','184A','194A']

INFLUXDB_ENABLED = 'S'
INFLUXDB_HOST = 'qwerty.com.ar'
INFLUXDB_PORT = 8086
INFLUXDB_USER = 'mim_tp1'
INFLUXDB_PASS = 'mim_tp1_transporte'
INFLUXDB_DBNAME = 'mim_tp1'
INFLUXDB_PROTOCOL = 'line'
TELEGRAM_ENABLED = 'S'
TELEGRAM_TOKEN = '971551324:AAGz8COn-WvxBWbbr_0N5bjeJVyIAAu487A'

def _url(path):
    return API_TRANSPORTE_URL + path

def filter_lines(data):
    result = []
    data = data.json()

    for element in LINEAS:
        for point in data:
            if element == point['route_short_name']:
               result.append(point)
    
    print('Size of data appended after filter function is : ' + str(len(result)))
    return result

def get_data(endpoint): 
    params = {
        'client_id': access_token,
        'client_secret': access_token_secret
        }
    
    # headers = requests.utils.default_headers()
    # headers['User-Agent'] = 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:70.0) Gecko/20100101 Firefox/70.0 '

    try:
        resp = requests.get(_url(endpoint),params=params) # headers=headers
        
        if resp.status_code == 200:
            logging.info('Status code: ' + str(resp.status_code))
            return filter_lines(resp)
    except:
        logging.error('timeout problem!')
        return None
    
    logging.error('Status code: ' + str(resp.status_code))
    return None
    
def store_data(data, counter, timestamp):
    df = pd.DataFrame(data)
    print('Printing dataframe to save: ' + str(df))
    table =  pandas_to_parquet(df)
    print('Saving .parquet file ' + PARQUET_PATH + 'bus_position_' + str(counter) + '.parquet!')
    pq.write_table(table, PARQUET_PATH + 'bus_position_' + str(counter) + '.parquet') # parquet file

def write_json_file(data, filename):
    f= open(filename,"w+")
    f.write(str(json.dumps(data)))
    f.close()

def pandas_to_parquet(dataframe):
    result = pa.Table.from_pandas(dataframe)

    return result

def save_json_data(data, counter, timestamp):
    print('Saving .json file for ' + JSON_PATH + 'bus_position_' + str(counter) + '.json!')
    write_json_file(data, JSON_PATH + 'bus_position_' + str(counter) + '.json')
 
def create_dir_structure():
    if not os.path.exists('reports/'):
        print('creating directory reports/')
        os.makedirs('reports/')
    else:
        print('directory reports/ exists!')

    if not os.path.exists('reports/json/'):
        print('creating directory reports/json/')
        os.makedirs('reports/json/')
    else:
        print('directory reports/json/ exists!')

    if not os.path.exists('reports/parquet/'):
        print('creating directory reports/parquet/')
        os.makedirs('reports/parquet/')
    else:
        print('directory reports/parquet/ exists!')
    
    print('###############################################')

def write_influxdb(host, port, user, password, dbname, protocol, filename):
    client = DataFrameClient(host, port, user, password, dbname)

    data = pd.read_json(filename)
    data['timestamp'] = pd.to_datetime(data['timestamp'])
    data = data.set_index('timestamp')
    # falta escribir tags
    print("Create database: " + dbname)
    if client.create_database(dbname):
        print('database created succesfully!')

    print("Write DataFrame")
    if client.write_points(data, 'transporte',protocol=protocol):
        print('data saved succesfully!')

def telegram_sendMessage(json_name, parquet_name):
    TelegramBot = telepot.Bot(TELEGRAM_TOKEN)
    msg_counter = 0
    msg = TelegramBot.getUpdates()
    for element in msg:
        for key, value in element.items():
            if 'message' in key:
                chat_id = str(value['chat']['id']) # catchear si no existe chat_id
                print('mensaje ' + str(msg_counter) + ': ' + str(value['text']))
                msg_counter += 1
    TelegramBot.sendMessage(chat_id=chat_id, parse_mode = 'html', text='<b>==========================</b> ')
    TelegramBot.sendMessage(chat_id=chat_id, parse_mode = 'html', text='<b>Nuevo archivo json creado:</b> ' + str(json_name))
    TelegramBot.sendMessage(chat_id=chat_id, parse_mode = 'html', text='<b>Nuevo archivo parquet creado:</b> ' + str(parquet_name))

def report_data(data, counter, timestamp):
    save_json_data(data,counter,timestamp) 

    JSON_FILE = JSON_PATH + 'bus_position_' + str(counter) + '.json'
    PARQUET_FILE = PARQUET_PATH + 'bus_position_' + str(counter) + '.parquet'  

    if 'S' in INFLUXDB_ENABLED:
        print('Writing data to influx...')
        try:
            write_influxdb(INFLUXDB_HOST, 
                INFLUXDB_PORT, 
                INFLUXDB_USER,
                INFLUXDB_PASS,
                INFLUXDB_DBNAME,
                INFLUXDB_PROTOCOL,
                JSON_FILE
            )
        except:
            print('Error trying to write data points into influxdb..')

    if ('S' in TELEGRAM_ENABLED) and (counter % 10 == 0):
        try:
            print('Sending telegram notification...')
            telegram_sendMessage(JSON_FILE, PARQUET_FILE)  
        except:
            print('Error trying to send telegram notification..')
    print('###############################################')    


if __name__ == '__main__':
    if len(sys.argv) != 4:
        print('Usage: <access_token> <access_token_secret> <first_file_no>')
        sys.exit(1)
    access_token = sys.argv[1]
    access_token_secret = sys.argv[2]
    counter = int(sys.argv[3])

    data = []
    create_dir_structure() 

    while True:
        now = datetime.now()
        if len(data) >= QRY_LENGTH:
            store_data(data, counter, now)
            report_data(data, counter, now)
            counter += 1
            data = []

        datum = get_data('/colectivos/vehiclePositionsSimple')
        
        if datum is not None:
            data.extend(datum)

        print('Size of filtered list is: ' + str(len(data)))
        print('###############################################')

        sleep(60)
