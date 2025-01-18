import random
import time
import uuid
from kafka import KafkaProducer
from configs import kafka_config
import json

# Створення Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
)

# Назва топіку
topic_name = 'oholodetskyi_building_sensors'

# Генерація випадкових даних датчика
sensor_id = str(uuid.uuid4())  # Унікальний ID для кожного запуску

for _ in range(100):
    # Генерація коректного timestamp
    current_timestamp = int(time.time() * 1000)  # Поточний час у мілісекундах
    data = {
        "sensor_id": sensor_id,
        "timestamp": current_timestamp,
        "temperature": random.randint(25, 45),
        "humidity": random.randint(15, 85),
    }
    producer.send(topic_name, value=data)
    producer.flush()
    print(f"Message sent: {data}")
    time.sleep(2)  # Інтервал між відправками
