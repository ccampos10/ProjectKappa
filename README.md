# Arquitectura Kapp
Arquitectura Kappa usando Kafka para la ingesta y Spark para el procesamiento de datos
(linux)
## Kafka
Herramienta descargada desde [la pagina oficial](https://kafka.apache.org/downloads) version 3.9.1, opcion binaria de Scala 2.13
- Iniciar Zookeeper
```bash
./zookeeper-server-start.sh ../config/zookeeper.properties
```
- Iniciar Kafka
```bash
./kafka-server-start.sh ../config/server.properties
```
- Crear topico "datos"
```bash
./kafka-topics.sh --create --topic datos --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```
## Spark
Herramienta descargada desde [la pagina oficial](https://spark.apache.org/downloads.html) version 4.0.1.  
Luego de descargar la herramienta, agregar la ruta /bin al path o variables de entorno
## Recomendacion
Despues de clonar el repositorio, abre una terminal en el archivo del repositorio y ejecutan
```bash
python -m venv envKappa
./envKappa/Scripts/activate
```

## Dependencias
- Version de Python: 3.13.7
- Version de Java: OpenJDK 17
- Librerias de python
    - Kafka-python 2.2.15
    - psutil 7.0.0
### Instalar dependencias de python
```bash
pip install -r requirements.txt
```

## Ejecutar el codigo
Para ejecutar el codigo, luego de inciar zookeeper y kafka. Ejecuta pcInfo.py en una terminal, y en otra aparte ejecuta
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.3.0 spark.py
```
