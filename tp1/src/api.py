import requests
import json
from time import sleep

API_URL = 'http://localhost:8086'
API_ENDPOINT = 'query'
INFLUX_DB = 'telegraf_test'

def _url(path):
    return API_URL + path

def get_influx_data(endpoint,db):
    params = {
        'q':'SELECT * FROM cpu',
        }
    resp = requests.get(_url(endpoint) + '?db=' + db,params=params)
    if resp.status_code != 200:
        # This means something went wrong.
        raise ValueError('GET ' + endpoint + ' error')
        
    return resp.json()

def insert_influx_data(endpoint, data=""):
    pass

def update_influx_data(endpoint, data=""):
    pass

while True:
    print(get_influx_data('/query',INFLUX_DB))
    sleep(1)