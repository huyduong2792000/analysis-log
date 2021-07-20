import os
import json
import time
from kafka import KafkaProducer

TOPIC = 'test-druid1'
BASE_PATH = "/data/2021-03-21/data/2021-03-21"
def get_log_files():
    result = os.listdir(os.path.expanduser(BASE_PATH))
    print ('Số file:', len(result))
    return result


def log_to_dict(raw_log):
    line = raw_log.split(' ')
    if len(line) > 10:
    # print (line)
        remote_ip = line[0]
        ip_client = line[1]
        time = line[2].replace('[', '').replace(']', '')
        domain = line[3]
        method = line[4]
        rote = line[5]
        http = line[6]
        status = line[7]
        data = line[8]
        link_unknow = line[9]
        browser = line[10]
        ip = line[-3]
        time_res = line[-2]

        result = {'remote_ip': remote_ip, 'ip_client': ip_client, 'time': time, 'domain': domain, 'method': method,
                        'rote': rote, 'http': http, 'status': status, 'res_size': data,
                        'link_unknow': link_unknow,
                        'browser': browser, 'upstream_addr': ip, 'res_duration': time_res}
        return result
    return None

def stream(limit = 1000):
    sent_count = 0
    if sent_count < limit:
        pass
    else:
        return
    json_producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v, indent = 4).encode('utf-8'))
    list_file = get_log_files()

    for file_name in list_file:
        print("===filename===", file_name)
        interval_count = 0
        with open(BASE_PATH + '/' + file_name) as json_log_file:
            for line in json_log_file:
                time.sleep(1)
                line_dict = log_to_dict(line)
                if line:
                    json_producer.send(TOPIC, line_dict)
                    interval_count += 1
                    sent_count += 1
                    if interval_count >= 15:
                        interval_count = 0
                        json_producer.flush()


stream(1000)