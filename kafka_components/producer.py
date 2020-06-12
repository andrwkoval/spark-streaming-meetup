import requests
from json import dumps, loads
from core.config import SERVERS

from kafka import KafkaProducer

stream = requests.get("http://stream.meetup.com/2/rsvps", stream=True)
producer = KafkaProducer(bootstrap_servers=SERVERS,
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

for i in stream.iter_lines():
    producer.send("events", value=loads(i))