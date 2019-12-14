FROM python:3.7.5-stretch

RUN mkdir -p /app/reports
RUN mkdir -p /app/reports/parquet
RUN mkdir -p /app/reports/json
RUN apt-get update
RUN apt-get install gcc
RUN apt-get install libsnappy-dev -y

COPY ./api_bus.py /app/
COPY ./__init__.py /app/
COPY ./requirements.txt /app/

WORKDIR /app

RUN pip3 install -r requirements.txt
# Montar volume para guardar los archivos de forma persistente
# guardar json y parquet dentro de una sola carpeta llamada reports con 2 subfolders
CMD ["python3", "api_bus.py", "fb174c1cde604a999877a85f1e69c18c", "d26E1dAb300B45DC9c752514AEf7C004", "266"]
