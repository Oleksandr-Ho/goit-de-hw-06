from kafka import KafkaConsumer
from configs import kafka_config
import json

# Назва топіку з алертами
alerts_topic = 'oholodetskyi_alerts'

# Створюємо Consumer для зчитування алертів із теми `oholodetskyi_alerts`
consumer = KafkaConsumer(
    alerts_topic,
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my_consumer_group_alerts'
)

# Обробка отриманих алертів
print(f"Subscribed to topic '{alerts_topic}'")
for message in consumer:
    alert = message.value
    print(f"Received alert: {json.dumps(alert, indent=4)}")

# Важливо: Consumer автоматично завершує роботу при завершенні програми
