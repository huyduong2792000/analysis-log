import os
import json

from kafka import KafkaConsumer
TOPIC = 'test-druid'
consumer = KafkaConsumer(TOPIC)
for msg in consumer:
    print (msg)
