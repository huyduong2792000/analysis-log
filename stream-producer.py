import os
import json
import time
from kafka import KafkaProducer

TOPIC = 'test-druid'
BASE_PATH = "/data/2021-03-21/data/2021-03-21"
def get_log_files():
    result = os.listdir(os.path.expanduser(BASE_PATH))
    print ('Số file:', len(result))
    return result

def stream(limit = 1000):
    sent_count = 0
    if sent_count < limit:
        pass
    else:
        return
    json_producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    list_file = get_log_files()

    for file_name in list_file:
        print("===filename===", file_name)
        interval_count = 0
        with open(BASE_PATH + file_name) as json_log_file:
            for line in json_log_file:
                if line:
                    json_producer.send(TOPIC, json.loads(line))
                    interval_count += 1
                    sent_count += 1
                    if interval_count >= 15:
                        interval_count = 0
                        producer.flush()


if __name__ == '__main__':
    stream(1000)