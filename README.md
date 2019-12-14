# MiM_SIyT

Grupo 14 

### Docentes
- Guido de Caso (gdecaso@gmail.com)
- Ignacio Pérez (ignacio.perez@gmail.com)

### Enlaces de interes

- https://graphql.org/code/#python
- https://swagger.io/tools/
- https://neo4j.com/developer/docker-run-neo4j/
- https://www.influxdata.com/products/

### tp1: api_bus.py

API_TRANSPORTE <-----HTTP REST -----> SCRIPT <-----PARQUET -----> FILE SYSTEM (1000 registriesxfile)

Filtrar solo 20 lineas seleccionadas. Se puede hacer en query al api o al armar el .parquet file.

```
pip3 install -r tp1/src/requirements.txt
```

```
sudo apt install protobuf-compiler
sudo apt install libsnappy-dev
```

Ejecutar script: 

``` 
python3 api_bus.py <access_token> <access_token_secret> <first_file_no>
```

Directorios con archivos json y parquet

```
- reports
|
|--- json
|--- parquet
    |---chunks

```

#### Ejecutar desde docker
```
docker build -t mim_tp1:1.x .
docker run -v ./reports:/app/reports mim_tp1:1.x
```

#### Protobuf
- https://developers.google.com/protocol-buffers/docs/pythontutorial
- https://www.youtube.com/watch?v=AW09fAsEb00&list=PLXY4_qxp8fUfML_FrAJN-OPfvg2DiqtIk

```
protoc -I=tp1/src/ --python_out=tp1/src/ tp1/src/message.proto
```

Se usan los metodos `SerializeToString()` y `ParseFromString(data)`

python write_message.py message.txt 
python read_message.py message.txt 

#### API Token GCBA Transporte
Para utilizar la API deben contar con las credenciales que se detallan a continuación. Es necesario agregarlas al request como query params.

```
client_id: fb174c1cde604a999877a85f1e69c18c
client_secret: d26E1dAb300B45DC9c752514AEf7C004
```

También puede navegar y obtener mas información sobre el uso desde la APIDoc: 

https://apitransporte.buenosaires.gob.ar/console/

#### Parquet
- https://arrow.apache.org/docs/python/parquet.html
- https://github.com/chhantyal/parquet-cli (pip install parquet-cli)

```
parq bus_position_2019-11-02\ 19\:40\:53.853531.parquet 

 # Metadata 
 <pyarrow._parquet.FileMetaData object at 0x7fefaeb15158>
  created_by: parquet-cpp version 1.5.1-SNAPSHOT
  num_columns: 11
  num_rows: 5349
  num_row_groups: 1
  format_version: 1.0
  serialized_size: 6073
```

```
parq bus_position_2019-11-02\ 19\:40\:53.853531.parquet  --schema

 # Schema 
 <pyarrow._parquet.ParquetSchema object at 0x7f751d2ec250>
route_id: INT64
latitude: DOUBLE
longitude: DOUBLE
speed: DOUBLE
timestamp: INT64 Timestamp(isAdjustedToUTC=false, timeUnit=microseconds, is_from_converted_type=false, force_set_converted_type=false)
id: INT64
direction: INT64
agency_name: BYTE_ARRAY String
agency_id: INT64
route_short_name: BYTE_ARRAY String
trip_headsign: BYTE_ARRAY String
```

```
parq input.parquet --count
5349
```

```
parq input.parquet --tail/head 10

```

#### influx
```
import os
import pandas as pd
from influxdb import DataFrameClient

INFLUXDB_HOST = 'qwerty.com.ar'
INFLUXDB_PORT = 8086
INFLUXDB_USER = 'admin'
INFLUXDB_PASS = 'lalal'
INFLUXDB_DBNAME = 'mim_tp1'
INFLUXDB_PROTOCOL = 'line'


files = os.listdir('/home/jgonzalez/dev/MiM_SIyT/tp1/src/reports')
data = pd.read_json('/home/jgonzalez/dev/MiM_SIyT/tp1/src/reports/bus_position__2019-11-06 00:26:02.636138.json')




client = DataFrameClient(INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USER, INFLUXDB_PASS, INFLUXDB_DBNAME)


data['timestamp'] = pd.to_datetime(data['timestamp'])
data = data.set_index('timestamp')
```

#### telegram bot
- https://khashtamov.com/en/how-to-create-a-telegram-bot-using-python/

### tp2: transform.py

Procesamiento de los datos recolectados con api_bus.py
