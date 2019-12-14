###################
#### LIBRARIES ####
###################

import requests
import json
import os
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
FILENAME = 'reports/bus_position_'
COUNT = 1
INFLUXDB_HOST = 'qwerty.com.ar'
INFLUXDB_PORT = 8086
INFLUXDB_USER = 'admin'
INFLUXDB_PASS = ''
INFLUXDB_DBNAME = 'mim_tp1'
INFLUXDB_PROTOCOL = 'line'
TELEGRAM_TOKEN = '971551324:AAGz8COn-WvxBWbbr_0N5bjeJVyIAAu487A'

###################
#### FUNCTIONS ####
###################

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
        print('printeo df entero')
        print(element)
        ###############
        df_split = split_dataframe(element)
        print('printeo df spliteado: ')
        print(df_split)
        for df in df_split:
            print(df)
        ###############
        tables.append(pa.Table.from_pandas(element))

    return tables

def split_pandas(data):
    size = 1000
    list_of_dfs = [data.loc[i:i+size-1,:] for i in range(0, len(data),size)]

    return list_of_dfs

def pandas_to_parquets(data):
    dfs = split_pandas(data) # split pandas df
    now = datetime.now()
    i=0
    for element in dfs:
        i += 1
        print('primer df: ' + str(element))
        element = pa.Table.from_pandas(element) # pandas to parquet
        pq.write_table(element, '../reports_parquet/bus_position_' + str(now) + '_' + str(i) + '.parquet') # parquet file

      

# MAIN

data = pd.read_json('../reports/bus_position__2019-11-12 19:42:20.426217.json')
print(data)

pandas_to_parquets(data)

# data_split = split_pandas(data)
# print(data_split)