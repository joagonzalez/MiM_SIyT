#!/usr/bin/python3

###################
#### LIBRARIES ####
###################

from pyarrow import parquet
import pandas
import pyarrow
from datetime import datetime
from natsort import natsorted

import glob
from geopy import distance

###############
#### CONST ####
###############

FMT = '%Y-%m-%d %H:%M:%S'
CHUNKS_PATH = 'reports/parquet/chunks/'
INPUT_PATH = 'reports/parquet/*.parquet'

###################
#### FUNCTIONS ####
###################

def build_batch(files):
    dataframes = []
    for file in files:
        dataframes.append(pyarrow.parquet.read_pandas(file).to_pandas())
    batch = pandas.concat(dataframes)
    return batch

def process_batch(batch, counter):
    
    # procesamos timestamps
    delta_time = delta_timestamp(batch)
    time_column = time_to_seconds(delta_time[0]) # en [0] esta la diff
    bus_lines = delta_time[3] # en [3] esta la route_short_names
    bus_route_id = delta_time[4] # en [4] esta la route_is
    day = delta_time[5] # # en [5] esta el dia
    distance = geo_distance(delta_time[1], delta_time[2]) # en [1] y [2] estan los df filtrados por timestamp min y max

    # print('#### DEBUG INIT #### \n')
    # print('DF batch filtrado por minimo timestamp')
    # print(delta_time[1])
    # print('DF batch filtrado por minimo timestamp')
    # print(delta_time[2])
    # print('El dia sobre el que trabaja el chunk es: ' + day)
    # print('segundos de viaje por cada linea en este chunk: \n' + str(time_column)) 
    # print('distancia recorrida por linea en metros en este chunk : ' + str(distance))
    # print('#### DEBUG END #### \n')

    # creamos data frame con informacion pre procesada de distance y tiempo de viaje
    data = {'route_short_names': bus_lines, 'route_id': bus_route_id, 'time': time_column, 'day': day, 'distance': distance}
    df_export = pandas.DataFrame(data)
    
    print('#### INFO PARQUET CHUNK ' + str(counter) + ' #### \n')
    print(df_export)

    store_data(df_export, counter) # exportamos df_export a parquet file

def process_transformed(files):
    # Con la información ya generada, se pueden calcular las respuestas
    dataframe = build_batch(files)
    # Hay que agrupar por las dimensiones necesarias y aplicar funciones de agregación:

    dataframe['speed']= dataframe['distance']/dataframe['time']
    
    speed_by_day=dataframe.groupby(['route_short_names','day','route_id'],as_index=False)['speed'].mean()
    faster_id=dataframe.groupby(['route_short_names','route_id'],as_index=False)['speed'].mean()
    faster_id= faster_id.loc[faster_id.groupby(['route_short_names', 'route_id'])['speed'].idxmax()]
    
    print('#### Velocidad por dia de cada linea: ')
    print(speed_by_day) # velocidad por día de cada línea
    print('#### Interno mas rapido por cada linea: ')
    print(faster_id) # interno más rápido de cada línea
    

def delta_timestamp(batch):
    # filtramos 
    df_maximo = batch.groupby(['route_short_name', 'route_id'], as_index=False).max(axis='timestamp')   
    df_minimo = batch.groupby(['route_short_name', 'route_id'], as_index=False).min(axis='timestamp')

    list_df_max = df_maximo.get('timestamp').tolist() # convertimos a lista columna timestamp de maximos
    list_df_min = df_minimo.get('timestamp').tolist() # convertimos a lista columna timestamp de minimos

    bus_lines = df_maximo.get('route_short_name').tolist() # convertimos a lista columna de lineas
    bus_route_id = df_maximo.get('route_id').tolist() # convertimos a lista columna de route_id

    # calculamos diff(timestamp_max, timestamp_min) para saber tiempo de recorrido por linea dentro del chunk 
    delta_time = [format_timestamp(i, FMT) - format_timestamp(j, FMT) for i, j in zip(list_df_max, list_df_min)] 
    
    # calculamos el dia
    current_date = datetime.fromtimestamp(list_df_max[0])
    day = str(current_date.day) + str(current_date.month) + str(current_date.year)
    
    # devolvemos lista de resultados
    return [delta_time,df_minimo,df_maximo, bus_lines, bus_route_id, day]

def format_timestamp(timestamp, timestamp_format):
    result = datetime.strptime(str(datetime.fromtimestamp(timestamp)), timestamp_format)
    return result

def time_to_seconds(list):
    result = []
    for element in list:
        seconds = element.total_seconds()
        element_result = seconds_to_dhms(seconds)
        result.append(element_result)
    
    return result

def seconds_to_dhms(seconds):
    days = seconds // (3600 * 24)
    hours = (seconds // 3600) % 24
    minutes = (seconds // 60) % 60
    seconds = seconds % 60
    return hours*3600 + minutes*60 + seconds
    
def geo_distance(min, max):
    long_max = max.get('longitude').tolist()
    long_min = min.get('longitude').tolist()

    lat_max = max.get('latitude').tolist()
    lat_min = min.get('latitude').tolist()

    points_max = merge_lists(lat_max, long_max)
    # print('puntos max: ' + str(points_max))
    points_min = merge_lists(lat_min, long_min)
    # print('puntos min: ' + str(points_min))
    result = merge_distance_points(points_max, points_min)
    # print('result:' + str(result))
    return result

def merge_lists(list1, list2): 
    merged_list = [(list1[i], list2[i]) for i in range(0, len(list1))] 
    return merged_list 

def merge_distance_points(list1, list2):
    merged_list = [distance.distance(list1[i], list2[i]).meters for i in range(0, len(list1))] 
    return merged_list

def store_data(data, counter):
    df = pandas.DataFrame(data)
    table =  pandas_to_parquet(df)
    print('Saving .parquet file ' + CHUNKS_PATH + 'pre_processed_chunk_' + str(counter) + '.parquet!')
    pyarrow.parquet.write_table(table, CHUNKS_PATH + 'pre_processed_chunk_' + str(counter) + '.parquet') # parquet file

def pandas_to_parquet(dataframe):
    result = pyarrow.Table.from_pandas(dataframe)

    return result

def speed_average_day():
    pass

def speed_max_line():
    pass

if __name__ == '__main__':
    all_files = natsorted(glob.glob(INPUT_PATH))
    print(INPUT_PATH)
    chunk_size = 50

    chunks = [all_files[i:i + chunk_size] for i in range(0, len(all_files), chunk_size)]    
    #print(str(chunks[117]))

    counter = 0
    for chunk in chunks:
        batch = build_batch(chunk)
        process_batch(batch, counter)
        counter += 1
        # if counter == 5:
        #     break
        
    final_files = natsorted(glob.glob(CHUNKS_PATH))
    process_transformed(final_files)