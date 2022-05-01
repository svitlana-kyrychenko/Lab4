FROM python:3.9-slim

RUN apt-get update

RUN pip install --upgrade pip

RUN pip install cassandra-driver

RUN pip install python-csv

COPY ./CassandraClient.py /opt/app/

ENTRYPOINT ["python", "/opt/app/CassandraClient.py"]