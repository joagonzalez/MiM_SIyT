from pyarrow import parquet
import pandas
import pyarrow
from datetime import datetime
from natsort import natsorted

import glob

from geopy import distance

def build_batch(files):
    dataframes = []
    for file in files:
        dataframes.append(pyarrow.parquet.read_pandas(file).to_pandas())
    batch = pandas.concat(dataframes)
    return batch

def process_batch(batch, counter):
    # Por cada interno hay que conseguir tiempo total y distancia total en el batch
    # y calcular en qué día ocurrió 

    # Con pandas se puede agrupar y aplicar funciones de agregación:
    # batch.groupby(['id', 'route_short_name']).max(axis='timestamp')

    # Con geopy se puede calcular la distancia usando las coordenadas:
    # point1 = (-34.56699, -58.438420)
    # point2 = (-34.53484, -58.467445)
    # distance.distance(point1, point2).meters

    # Con datetime se puede calcular el día a partir del timestamp
    # from datetime import datetime
    # date = datetime.fromtimestamp(1574296430)
    # date.year, date.month, date.day

    # Finalmente hay que escribir un nuevo parquet con toda la información generada
    # parquet.write_table(table, 'transformed_' + str(counter))
    
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
    
if __name__ == '__main__':
    input_path = './*.parquet'
    all_files = natsorted(glob.glob(input_path))
    chunk_size = 50
    chunks = [all_files[i:i + chunk_size] for i in range(0, len(all_files), chunk_size)]
    
    counter = 0
    for chunk in chunks:
        batch = build_batch(chunk)
        process_batch(batch, counter)
        counter += 1
