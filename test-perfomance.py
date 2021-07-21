# import re
# conf = '$remote_ip $ip_client [$time] $domain "$method $link_unknow $http" $status $res_size "$http_referer" "$http_user_agent" - $unknown $res_time "$unknown1" $upstream_addr'
# # log = '192.168.241.119 14.169.52.225 [2021-03-21T23:58:54+07:00] cafebiz.vn "GET /ong-trum-kin-tieng-thi-cong-thiet-ke-cho-hang-loat-chuoi-the-coffee-house-bach-hoa-xanh-fpt-vingroup-flc-moi-3-nam-tuoi-doanh-thu-vai-tram-ty-dong-nam-20210313224854838.chn HTTP/1.1" 200 156270 "-" "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; MDDC)" - 695 192.168.241.108:7695 0.002 -'
# log = '192.168.6.49 148.251.69.139 [2021-03-21T09:21:25+07:00] cache_phunuvietnam "GET /tim-kiem/?key=v%C6%B0%E1%BB%A3t%20qua%20d%C6%B0%20lu%E1%BA%ADn HTTP/1.0" 301 380 "-" "Mozilla/5.0 (compatible; MJ12bot/v1.4.8; http://mj12bot.com/)" - 535 0.009 "23" 10.5.24.25:9210'
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

list_cofig = [
    '$remote_addr $http_x_forwarded_for [$time_iso8601] $http_host "$request" $status $bytes_sent "$http_referer" "$http_user_agent" $gzip_ratio $request_length $upstream_addr $request_time',
    '$remote_addr $http_x_forwarded_for [$time_iso8601] $http_host "$request" $status $bytes_sent "$http_referer" "$http_user_agent" $gzip_ratio $request_length $request_time "$sent_http_servername" $upstream_addr',
    '$remote_addr $http_x_forwarded_for [$time_iso8601] $http_host "$request" $status $bytes_sent "$http_referer" "$http_user_agent" $gzip_ratio $request_length $request_time $upstream_addr $srcache_fetch_status $srcache_store_status',
]

list_regex = []

for conf in list_cofig:
    regex = ''.join(
        '(?P<' + g + '>.*)' if g else re.escape(c)
        for g, c in re.findall(r'\$(\w+)|(.)', conf))
    list_regex.append(regex)

def get_log_files():
    result = os.listdir(os.path.expanduser(BASE_PATH))
    print ('Số file:', len(result))
    return result

def log_to_dict(raw_log = None):
    # raw_log = '192.168.6.220 14.228.146.177 [2021-03-21T01:47:44+07:00] m.gamek.vn "GET /ajax-count-commnent.chn HTTP/1.1" 200 296 "https://m.gamek.vn/faker-032010295194.chn" "Mozilla/5.0" - 2232 192.168.5.35:8510 0.018'
    print("===raw log===", raw_log)
    for regex in list_regex:
        m = re.match(regex, raw_log)
        if m:
            return m.groupdict()
    return None
def stream(limit = 1000):
    # json_producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v, indent = 4).encode('utf-8'))
    list_file = get_log_files()

    for file_name in list_file[1100: 1299]:
        print("===filename===", file_name)
        with open(BASE_PATH + '/' + file_name) as json_log_file:
            for line in json_log_file:
                line_dict = log_to_dict(line)
                print("==line_dict===", line_dict)
                # if line:
                #     json_producer.send(TOPIC, line_dict)
                #     json_producer.flush()


stream(1000)
# log_to_dict()