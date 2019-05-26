# SparkWeather

Applicazione Spark per il processamento distribuito di big data riguardanti rilevazioni meteorologiche. 

## Getting Started

### Prerequisites

* [Apache Spark](https://spark.apache.org/)
* [Apache NiFi](https://nifi.apache.org/)
* [Apache Hadoop (HDFS)](http://hadoop.apache.org/)
* [Mongo DB](https://www.mongodb.com/)
* [Python 3.x](https://www.python.org/)
* Python libraries: [pytz](https://pypi.org/project/pytz/), [timezonefinder](https://pypi.org/project/reverse_geocode/), [reverse-geocode](https://pypi.org/project/reverse_geocode/)
* [Docker](https://www.docker.com/) (opzionale)
* [Docker Compose](https://docs.docker.com/compose/) (opzionale)

### Installing

È possibile ottenere l'ambiente di esecuzione installando in locale i framework sopra elencati oppure sfruttando gli script disponibili nella cartella [dist](dist) per eseguire le relative versioni "containerizzate".

Ad esempio, come primo passo si esegua lo script [start-hdfs.sh](dist/start-hdfs.sh) per lanciare un una versione pseudo-distribuita di HDFS
```
$ dist/start-hdfs.sh

d4d9ebd66ff2d4f153fd9cfa2d64a65b6b1a60a6e5f6c8b1401a4421e69895b2
33f8cae5a3c1a813322721cb4fd3e56e1ca392b98ed7b67279c646168ab590b4
d956986540ec81ef347b22392006471b8189456e1a585d62ea9cafeeec19798a
73fb75458f39182cf7116d15250bb27a90a96f2597c69b0aecde3457fe36ce47
sudo: /usr/local/hadoop/etc/hadoop/hadoop-env.sh: command not found
rm: cannot remove '/tmp/*.pid': No such file or directory
 * Starting OpenBSD Secure Shell server sshd                             [ OK ] 
root@b5b7551e9fe5:/# 
```
e lo script [init_hdfs.sh](dist/data/hdfs/init_hdfs.sh) per inizializzare namenode ed i 3 datanode che fungono da worker
```
root@b5b7551e9fe5:/# /data/init_hdfs.sh

WARNING: /usr/local/hadoop/logs does not exist. Creating.
2019-05-26 19:38:18,937 INFO namenode.NameNode: STARTUP_MSG: 
/************************************************************
STARTUP_MSG: Starting NameNode
STARTUP_MSG:   host = b5b7551e9fe5/172.18.0.5
STARTUP_MSG:   args = [-format]
STARTUP_MSG:   version = 3.1.2
STARTUP_MSG:   classpath = ...
...
Starting namenodes on [master]
Starting datanodes
slave1: WARNING: /usr/local/hadoop-3.1.2/logs does not exist. Creating.
slave3: WARNING: /usr/local/hadoop-3.1.2/logs does not exist. Creating.
slave2: WARNING: /usr/local/hadoop-3.1.2/logs does not exist. Creating.
Starting secondary namenodes [b5b7551e9fe5]
root@b5b7551e9fe5:/#
```
infine lo script [create_topics_env.sh](dist/data/hdfs/create_topics_env.sh) per creare le cartelle su cui verrànno salvati i file a seguito della fase di data ingestion ed i risultati di output del processamento.
```
root@b5b7551e9fe5:/# /data/create_topics_env.sh
```
Inizializzato lo strato di storage, è possibile eseguire ed aggiungere alla medesima rete virtuale i container relativi a NiFi e Spark tramite il seguente comando:
```
SparkWeather/dist$ docker-compose -d up --scale spark-worker=2
```
L'opzione `--scale spark-worker=2` permette di eseguire 2 nodi worker per il processamento dei dati.

*Nota:* affinché l'applicazione venga eseguita correttamente è necessario installare nel sistema o nei container Spark le librerie python presenti nel file [requirements.txt](requirements.txt) ovvero  *pytz*, *timezonefinder* e *reverse-geocode*, l'installazione può essere effettuata ad esempio tramite *pip*. Altrimenti è sufficiente specificare il riferimento ai relativi package tramite l'opzione `--py-files` in fase di esecuzione dell'applicazione.


*Nota:* il file [docker-compose.yml](dist/docker-compose.yml) contiene anche le configurazioni di altri container basati su Kafka, tali configurazioni sono opportunamente commentate poiché non si è riuscito ad impiegare il framework ed i suoi connettori in modo stabile.

## Running

### Data pre-Processing and Ingestion

Per eseguire l'ingestion ed il preprocessamento è possibile accedere da browser all'indirizzo http://localhost:8080/nifi, caricare il template [ingestion-stamble.xml](dist/data/nifi/ingestion-stamble.xml) con il pulsante apposito inserirlo nel foglio di lavoro tramite drag and drop premere play nel foglio: *"ingestion: HDD to HDFS"*

### Data Processing

Per avviare l'applicazione Spark basta eseguire i seguenti script tramite il comando `spark-submit`:

[run.py](run.py) esegue la *query 1* i cui risultati vengono archiviati in hdfs all'interno di *topics/out/query1/*
```
$ $SPARK_HOME/bin/spark-submit run.py
```
[run2.py](run2.py) esegue la *query 2* i cui risultati vengono archiviati in hdfs all'interno di *topics/out/query2/*
```
$ $SPARK_HOME/bin/spark-submit run2.py
```
[run3.py](run3.py) esegue la *query 3* i cui risultati vengono archiviati in hdfs all'interno di *topics/out/query3/*
```
$ $SPARK_HOME/bin/spark-submit run3.py
```
[query1.py](query1.py) esegue la *query 1* i cui risultati vengono archiviati in hdfs all'interno di *topics/out/query1sql/*
```
$ $SPARK_HOME/bin/spark-submit query1.py
```
[query2.py](query2.py) esegue la *query 2* i cui risultati vengono archiviati in hdfs all'interno di *topics/out/query2sql/*
```
$ $SPARK_HOME/bin/spark-submit query2.py
```

### Data Migration

Per eseguire l'ingestion dei risultati da hdfs a mongodb tornare in *NiFi* e premere play nel foglio: *"ingest: HDFS to MongoDB"*.

I risultati vengono automaticamente salvati nel database chiamato *"db"* e nelle collection *query1*, *query2* e *query3*.

### Shutting Down

Per terminare il sistema è possibile sfruttare i seguenti script presenti nella cartella [dist](dist):
```
$ docker-compose down
$ ./stop-hdfs.sh
$ ./stop-mongodb-server.sh	
```

## Authors

* **Aldo Pietrangeli**
* **Simone Falvo**

## Acknowledgments
Immagini Docker:
* https://github.com/effereds/hadoop
* https://github.com/smvfal/pynifi
* https://github.com/smvfal/spark

Librerie Python
* [pytz](https://pypi.org/project/pytz/)
* [timezonefinder](https://pypi.org/project/reverse_geocode/)
* [reverse-geocode](https://pypi.org/project/reverse_geocode/)
* [pymongo](https://pypi.org/project/pymongo/)
* [pandas](https://pandas.pydata.org/)

Connettori Kafka
* https://www.confluent.io/
