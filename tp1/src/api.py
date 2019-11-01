import requests
import json
from time import sleep

API_TRANSPORTE_URL = 'https://apitransporte.buenosaires.gob.ar'
CLIENT_ID = 'fb174c1cde604a999877a85f1e69c18c'
CLIENT_SECRET = 'd26E1dAb300B45DC9c752514AEf7C004'
FILENAME = 'bike_stations.json'
COUNT = 1

def _url(path):
    return API_TRANSPORTE_URL + path

def get_trasporte(endpoint):
    params = {
        'client_id': CLIENT_ID,
        'client_secret':CLIENT_SECRET
        }
    
    resp = requests.get(_url(endpoint),params=params)
    
    if resp.status_code != 200:
        # This means something went wrong.
        raise ValueError('GET ' + endpoint + ' error')
        
    return resp.json()

def show_results(data):
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

#### MAIN PROGRAM 

threshold = input('Ingrese cantidad de iteraciones: ')

while True:
    data = get_trasporte('/ecobici/gbfs/stationStatus')
    
    save_data(data,FILENAME)

    show_results(data)
    print('#############')
    print('Query #' + str(COUNT))
    print('#############')
    COUNT += 1
    if COUNT > threshold:
        break
    sleep(2)