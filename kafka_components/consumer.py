from json import loads
from core.config import SERVERS

from kafka import KafkaConsumer, TopicPartition

def all_events(topic):
    result = []
    consumer = KafkaConsumer(topic,
                             bootstrap_servers=SERVERS,
                             value_deserializer=lambda m: loads(m.decode('utf-8')),
                             auto_offset_reset="earliest",
                             enable_auto_commit=False)
    partition = TopicPartition(topic, partition=0)
    end_offset = consumer.end_offsets([partition])[partition]

    i = consumer.next()
    while i.offset != end_offset - 1:
        result.append(i.value)
        i = consumer.next()
    return result
