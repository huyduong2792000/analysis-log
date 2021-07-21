# import re
# conf = '$remote_ip $ip_client [$time] $domain "$method $link_unknow $http" $status $res_size "$http_referer" "$http_user_agent" - $unkown $upstream_addr $res_time -'
# log = '192.168.241.119 14.169.52.225 [2021-03-21T23:58:54+07:00] cafebiz.vn "GET /ong-trum-kin-tieng-thi-cong-thiet-ke-cho-hang-loat-chuoi-the-coffee-house-bach-hoa-xanh-fpt-vingroup-flc-moi-3-nam-tuoi-doanh-thu-vai-tram-ty-dong-nam-20210313224854838.chn HTTP/1.1" 200 156270 "-" "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; MDDC)" - 695 192.168.241.108:7695 0.002 -'
# regex = ''.join(
#     '(?P<' + g + '>.*?)' if g else re.escape(c)
#     for g, c in re.findall(r'\$(\w+)|(.)', conf))
# m = re.match(regex, log)
# print(m.groupdict())
#1223

import re
import os
import json
import time
from kafka import KafkaProducer

TOPIC = 'test-druid4'
BASE_PATH = "/data/2021-03-21/data/2021-03-21"
conf = '$remote_ip $ip_client [$time] $domain "$method $link_unknow $http" $status $res_size "$http_referer" "$http_user_agent" - $unkown $upstream_addr $res_time -'
regex = ''.join(
    '(?P<' + g + '>.*?)' if g else re.escape(c)
    for g, c in re.findall(r'\$(\w+)|(.)', conf))

def get_log_files():
    result = os.listdir(os.path.expanduser(BASE_PATH))
    print ('Số file:', len(result))
    return result


def log_to_dict(raw_log):
    m = re.match(regex, raw_log)
    return m.groupdict()

def stream(limit = 1000):
    json_producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v, indent = 4).encode('utf-8'))
    list_file = get_log_files()

    for file_name in list_file[1000: 1220]
        print("===filename===", file_name)
        with open(BASE_PATH + '/' + file_name) as json_log_file:
            for line in json_log_file:
                line_dict = log_to_dict(line)
                print("==line_dict===", line_dict)
                # if line:
                #     json_producer.send(TOPIC, line_dict)
                #     json_producer.flush()


stream(1000)