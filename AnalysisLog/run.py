import re
import json
from multiprocessing import Process, Manager
import time
import itertools 
from kafka import KafkaProducer

TOPIC = 'test-druid5'
json_producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v, indent = 4).encode('utf-8'))

conf = '$remote_addr $http_x_forwarded_for [$time_iso8601] $http_host "$request" $status $bytes_sent "$http_referer" "$http_user_agent" $rest_value'
regex = ''.join(
        '(?P<' + g + '>.*)' if g else re.escape(c)
        for g, c in re.findall(r'\$(\w+)|(.)', conf))

def parse_log_to_dict(raw_log = None):
    m = re.match(regex, raw_log)
    if m:
        return m.groupdict()
    return {}

def stream(raw_log):
    dict_log = parse_log_to_dict(raw_log)
    print("===dict_log===", dict_log)
    if dict_log:
        future = json_producer.send(TOPIC, dict_log)
        json_producer.flush()
def do_work(in_queue):
    while True:
        line = in_queue.get()
        print("===line===", line)
        # stream(line)


if __name__ == "__main__":
    num_workers = 4

    manager = Manager()
    results = manager.list()
    work = manager.Queue(num_workers)

    with open("/home/vunm/analysis-log/msoha-27-rq.log") as logs:
        # iters = itertools.chain(f, (None,)*num_workers)
        for line in logs:
            print("===line===", line)
            work.put(line)
    # start for workers    
    pool = []
    for i in range(0, num_workers):
        p = Process(target=do_work, args=(work))
        p.start()
        pool.append(p)

    # produce data

    for p in pool:
        p.join()

    # print(results)