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
CLIENT_ID = 'fb174c1cde604a999877a85f1e69c18c'
CLIENT_SECRET = 'd26E1dAb300B45DC9c752514AEf7C004'
FILENAME = 'reports/json/bus_position_'
COUNT = 1
INFLUXDB_HOST = 'qwerty.com.ar'
INFLUXDB_PORT = 8086
INFLUXDB_USER = 'mim_tp1'
INFLUXDB_PASS = 'mim_tp1_transporte'
INFLUXDB_DBNAME = 'mim_tp1'
INFLUXDB_PROTOCOL = 'line'
TELEGRAM_TOKEN = '971551324:AAGz8COn-WvxBWbbr_0N5bjeJVyIAAu487A'
LINEAS = ['29A','41A','44A','57A','59A','63A','65A','67A','68A','80A','107A','113A','133A','152A','161A','168A','184A','194A']
# '60S','60T','60L','60H','60K','60R','194D','194E','194F','194G','194H','194I','194J','57B','68B','168C','168D','107B','80B','65B','59C','161B','168B','194B','63B','113B','152B','59B','29B','60U',
def _url(path):
    return API_TRANSPORTE_URL + path

def filter_lines(data):
    result = []
    data = data.json()

    for element in LINEAS:
        for point in data:
            if element == point['route_short_name']:
               result.append(point)
               print('appendeo punto a lista filtrada: ' + str(point))
    
    print('el tamaño del append es: ' + str(len(result)))
    return result

def get_data(endpoint): 
    params = {
        'client_id': access_token,
        'client_secret': access_token_secret
        }
    
    resp = requests.get(_url(endpoint),params=params)
    
    if resp.status_code != 200:
        raise ValueError('GET ' + endpoint + ' error')
    
    # aca hay que filtrar las 20 lineas
    data = filter_lines(resp)
    
    return data

def store_data(data, counter):
    # aca pasamos a pandas y luego a parquet la lista json que nos viene de queries con API
    print(str(counter) + ': ' + str(data))

def write_json_file(data, filename):
    f= open(filename,"w+")
    f.write(str(json.dumps(data)))
    f.close()

def json_to_pandas(path, timestamp):
    list = []

    files = os.listdir(path)

    for file in files: # json file to pandas
        if str(timestamp) in file:
            list.append(pd.read_json(path + file))
        
    return list

def pandas_to_parquet(data):
    tables = []
    for element in data: # pandas to parquet
        print(element)
        tables.append(pa.Table.from_pandas(element))

    return tables
 
def create_dir_structure():
    if not os.path.exists('reports/'):
        print('creando directorio reports/')
        os.makedirs('reports/')
    else:
        print('directorio reports/ existe!')

    if not os.path.exists('reports/json/'):
        print('creando directorio reports/json/')
        os.makedirs('reports/json/')
    else:
        print('directorio reports/json/ existe!')

    if not os.path.exists('reports/parquet/'):
        print('creando directorio reports/parquet/')
        os.makedirs('reports/parquet/')
    else:
        print('directorio reports/parquet/ existe!')
  
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
        if len(data) >= 1000:
            store_data(data, counter)
            counter += 1
            data = []

        datum = get_data('/colectivos/vehiclePositionsSimple')
        data.append(datum)
        write_json_file(data,'test_' + str(counter) + '.json')

        sleep(10)
