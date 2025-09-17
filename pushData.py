from kafka import KafkaProducer as kp
import json
import time

producer = kp(bootstrap_servers=['localhost:9092'], value_serializer=lambda v: json.dumps(v).encode('utf-8'))

terminal = int(input("ingresa la terminal: "))
delay = int(input("ingresa el delay: "))


for i in range(21):
    dato = {
        'id': i,
        'terminal': terminal,
        'usuario': f'user_{i%3}',
        'time': time.time()
    }
    producer.send('datos', dato)
    print(dato)
    time.sleep(delay)

producer.flush()
