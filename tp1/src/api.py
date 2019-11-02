###################
#### LIBRARIES ####
###################

import requests
import json
import os
from time import sleep
from datetime import datetime

import pyarrow.parquet as pq
import numpy as np
import pandas as pd
import pyarrow as pa

###############
#### CONST ####
###############

API_TRANSPORTE_URL = 'https://apitransporte.buenosaires.gob.ar'
CLIENT_ID = 'fb174c1cde604a999877a85f1e69c18c'
CLIENT_SECRET = 'd26E1dAb300B45DC9c752514AEf7C004'
#FILENAME = 'reports/bike_stations_'
FILENAME = 'reports/bus_position_'
COUNT = 1

###################
#### FUNCTIONS ####
###################

def _url(path):
    return API_TRANSPORTE_URL + path

def get_transporte(endpoint):
    params = {
        'client_id': CLIENT_ID,
        'client_secret':CLIENT_SECRET
        }
    
    resp = requests.get(_url(endpoint),params=params)
    
    if resp.status_code != 200:
        # This means something went wrong.
        raise ValueError('GET ' + endpoint + ' error')
        
    return resp.json()

def show_results_bus(data):
    for bus in data:
        for key, value in bus.items():
            print('key: ' + str(key) + 'value: ' + str(value))

def show_results_bike(data):
    for key, value in data.items():
        print('key: ' + str(key))
        if 'data' in key:
            for key, value in value.items():
                print('key: ' + str(key))
                for station in value:
                    for key, value in station.items():
                        if 'station_id' in key:
                            print('#### STATION: ' + value + ' ####')
                        print(str(key) + ' - ' + 'value: ' + str(value))
        else:
            print('value: ' + str(value))

def save_data(data, filename):
    f= open(filename,"w+")
    f.write(str(json.dumps(data)))
    f.close()

def write_parquet_message(path):
    list = []
    tables = []

    files = os.listdir(path)

    for file in files: # json file to pandas
        list.append(pd.read_json(path + file))

    for element in list: # pandas to parquet
        print(element)
        tables.append(pa.Table.from_pandas(element))
    
    for table in tables: # .parquet file 
        now = datetime.now()
        #print(table)
        pq.write_table(table, 'reports/bus_position_' + str(now) + '.parquet')

def read_parquet_message(path):
    pass

######################
#### MAIN PROGRAM ####
######################

threshold = input('Ingrese cantidad de iteraciones: ')

while True:
    #data = get_trasporte('/ecobici/gbfs/stationStatus')
    data = get_transporte('/colectivos/vehiclePositionsSimple')
    now = datetime.now()
    save_data(data,FILENAME + '_' + str(now) + '.json')

    #show_results_bike(data)
    show_results_bus(data)
    print('#############')
    print('Query #' + str(COUNT))
    print('#############')
    COUNT += 1
    if COUNT > int(threshold):
        break
    sleep(2)

print('Writing .parquet files....')
write_parquet_message('reports/')