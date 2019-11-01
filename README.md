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
```
pip install -r tp1/src/requirements.txt
```

```
sudo apt install protobuf-compiler```
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