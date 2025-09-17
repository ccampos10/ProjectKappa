from kafka import KafkaConsumer as kc
import json

consumer = kc('datos',
              bootstrap_servers=['localhost:9092'],
              value_deserializer=lambda m: json.loads(m.decode('utf-8')),
              auto_offset_reset='earliest',
              enable_auto_commit=True)

print("datos..")
for dato in consumer:
    print(dato.value)
