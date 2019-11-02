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

### tp1

API_TRANSPORTE <-----HTTP REST -----> SCRIPT <-----PARQUET -----> FILE SYSTEM

Filtrar solo 20 lineas seleccionadas. Se puede hacer en query al api o al armar el .parquet file.

```
pip install -r tp1/src/requirements.txt
```

```
sudo apt install protobuf-compiler
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
