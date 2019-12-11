#!/usr/bin/python3

from pyarrow import parquet
import pandas
import pyarrow
from datetime import datetime
from natsort import natsorted

import glob

# from geopy import distance

def build_batch(files):
    dataframes = []
    for file in files:
        dataframes.append(pyarrow.parquet.read_pandas(file).to_pandas())
    batch = pandas.concat(dataframes)
    # devuelve 1 df con los datos de 50 archivos concatenados (chunk)
    return batch

def process_batch(batch, counter):
    # Por cada interno hay que conseguir tiempo total y distancia total en el batch
    # y calcular en qué día ocurrió 

    # Con pandas se puede agrupar y aplicar funciones de agregación:
    print('######## BATCH PROCESADO #############')
    print('maximo!')
    print(batch.groupby(['id', 'route_short_name'], as_index=False).max(axis='timestamp'))
    print('minimo')
    print(batch.groupby(['id', 'route_short_name'], as_index=False).min(axis='timestamp'))
    id  = '27455'
    
    # for index, row in batch.iterrows():
    #     if row['id'] == id: 
    #         print (str(row["id"]) + ' ' + str(row["route_short_name"]) + ' ' + str(row["timestamp"]) + ' ' + ' aparece!')
    # Con geopy se puede calcular la distancia usando las coordenadas:
    # point1 = (-34.56699, -58.438420)
    # point2 = (-34.53484, -58.467445)
    # distance.distance(point1, point2).meters

    # Con datetime se puede calcular el día a partir del timestamp
    date = datetime.fromtimestamp(1574296430)
    date.year, date.month, date.day
    print('imprimimos fecha: ' + str(date.year) + str(date.month) + str(date.day))
    # Finalmente hay que escribir un nuevo parquet con toda la información generada
    # parquet.write_table(table, 'transformed_' + str(counter))
    
    # armamos algo asi como salida de la funcion
    # [
    #   {
    #     "distance": -34.65946,
    #     "time": 1234,
    #     "day": 1572732090,
    #     "id": "1822",
    #     "route_short_name": "253A"
    #   }
    # ]

    # tiempo max del batch para cada linea - tiempo min del batch por linea = tiempo
    # distance = posicion tiempo max  - posicion tiempo min
    # si el batch empieza en dia A pero termina en dia B contamos como recorrido de dia B?
    #  
    print('processed batch')


def process_transformed(files):
    # Con la información ya generada, se pueden calcular las respuestas
    dataframe = build_batch(files)
    
    # Hay que agrupar por las dimensiones necesarias y aplicar funciones de agregación:
    # dataframe.groupby(['route_short_name','day']).seconds.sum()
    # dataframe.groupby(['route_short_name','day']).distance.sum()
    # Y así calcular la velocidad de cada bloque

    # Imprimir las respuestas para
    # - velocidad por día de cada línea
    # - interno más rápido de cada línea
    
def speed_average_day():
    pass
def speed_max_line():
    pass
if __name__ == '__main__':
    input_path = '/code/MiM_SIyT/src/reports/parquet/*.parquet'
    all_files = natsorted(glob.glob(input_path))
    print(input_path)
    chunk_size = 50

    chunks = [all_files[i:i + chunk_size] for i in range(0, len(all_files), chunk_size)]    
    print(str(chunks))

    counter = 0
    for chunk in chunks:
        batch = build_batch(chunk)
        print('######### BATCH SIN PROCESAR ############')
        print('Imprimimos dataframe del batch ' + str(counter))
        print(str(batch['id']))
        process_batch(batch, counter)
        counter += 1
