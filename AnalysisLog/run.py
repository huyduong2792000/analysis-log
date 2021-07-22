import re
# import os
import json
import time
from kafka import KafkaProducer
from manager import Manager

from os import listdir
from os.path import isfile, join
# onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]

manager = Manager()

json_producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v, indent = 4).encode('utf-8'))
log_dir = '/home/vunm/analysis-log'
TOPIC = 'test-druid5'

list_cofig = [
    '$remote_addr $http_x_forwarded_for [$time_iso8601] $http_host "$request" $status $bytes_sent "$http_referer" "$http_user_agent" $rest_value'
]
list_regex = []

for conf in list_cofig:
    regex = ''.join(
        '(?P<' + g + '>.*)' if g else re.escape(c)
        for g, c in re.findall(r'\$(\w+)|(.)', conf))
    list_regex.append(regex)


def load_files(log_dir):
    onlyfiles = [f for f in listdir(log_dir) if isfile(join(log_dir, f))]
    print ('Số file:', len(onlyfiles))
    return onlyfiles

def parse_log_to_dict(raw_log = None):
    for regex in list_regex:
        m = re.match(regex, raw_log)
        if m:
            return m.groupdict()
    return None

def stream(raw_log, file_name):
    dict_log = parse_log_to_dict(raw_log)
    dict_log['file_name'] = file_name
    if dict_log:
        future = json_producer.send(TOPIC, dict_log)
        result = future.get(timeout=60)
        print("===result===", result)
        json_producer.flush(30)
@manager.command
def run():
    files = load_files(log_dir)
    print("===", files)
    for file_name in files:
        with open('{log_dir}/{file_name}'.format(log_dir = log_dir, file_name = file_name)) as logs:
            for line in logs:
                stream(line, file_name)

if __name__ == '__main__':
    manager.main()