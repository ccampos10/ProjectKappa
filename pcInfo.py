from kafka import KafkaProducer as kp
import json
import psutil
import time
import signal
from socket import gethostname

def closeConnection(sig, frame):
    producer.flush()
    exit()

signal.signal(2, closeConnection) # control + c

try:
    oldPacketsSent = psutil.net_io_counters().packets_sent
    time.sleep(1)
    producer = kp(bootstrap_servers=['localhost:9092'], value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    pcID = gethostname()
    print(f"ID del equipo {pcID}")

    while True:
        actPacketsSent = psutil.net_io_counters().packets_sent
        packetsPerSecond = actPacketsSent - oldPacketsSent
        cpuUsage = psutil.cpu_percent(interval=1)
        ramUsage = psutil.virtual_memory().percent
        print('uso del cpu: ',cpuUsage)
        print('uso de la ram: ',ramUsage)
        print('Paquetes enviados por segundo: ', packetsPerSecond)
        oldPacketsSent=actPacketsSent
        data = {
            'pc_id': pcID,
            'cpu_usage': cpuUsage,
            'ram_usage': ramUsage,
            'packets_per_second': packetsPerSecond,
            'time': time.time()
        }
        producer.send('datos', data)
        time.sleep(1)
except ValueError as ve:
    print("Dato ingresado invalido")
except Exception as e:
    print("A ocurrido un error...")
    print(e)
