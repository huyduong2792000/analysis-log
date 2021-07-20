#!/usr/bin/python
# -*- coding: utf-8 -*-
# -- vunm --

# 192.168.241.119 115.84.183.195 [2021-03-21T23:58:54+07:00] afamily.vn "GET /khoanh-khac-cu-ong-bi-dot-quy-khi-dang-sac-dien-thoai-nguoi-nha-ngoi-ngay-ben-canh-ma-khong-biet-20210320233823893.chn HTTP/1.1" 200 29805 "http://afamily.v$
# 192.168.241.121 116.110.40.124 [2021-03-21T23:58:56+07:00] afamily.vn "GET /web_images/css_spritesv1.png HTTP/1.1" 200 34759 "https://afamily.vn/minify/main2-13032021v1.min.css?ver=637519670952408750" "Mozilla/5.0 (Windows NT 6.1; W$
import os
import json
def list_file():
    # path = "/data/2021-03-21/data/2021-03-21"
    path = "/data/2021-03-21/data/2021-03-21"
    result = os.listdir(os.path.expanduser(path))
    print ('Số file:', len(result))
    return result
def get_file_size(filename):
    # b = os.path.getsize("/data/2021-03-21/data/2021-03-21/sport5-120-rq.log")
    file_size = os.path.getsize("/data/2021-03-21/data/2021-03-21/{}".format(filename.replace('\n',''))) #file_size
    return file_size
def file_not_null():
    result = []
    all_file = list_file()
    file_json = open("fileNotNull.txt", "a+")
    for file in all_file:
        if get_file_size(file) > 0:
            file_json.write(file + '\n');
            result.append(file)
    file_json.close()
    return result

def list_file_by_quota(quota = 10737418240):
    #     10737418240
    result = []
    total = 0
    file_json = open("file20G.txt", "a+")
    with open('fileNotNull.txt') as lines:
        for file in lines:
            if get_file_size(file.replace('\n','')) > 0:
                if total < quota:
                    total += get_file_size(file)
                    file_json.write(file);
                    result.append(file)
                else:
                    break
    file_json.close()
    return result

def convert_file():
    with open('file20G.txt') as lines:
        for file in lines:
            file_json = open("./data20G/{}.json".format(file.replace('\n','')), "a+") # mowr file de ghi

            with open('/data/2021-03-21/data/2021-03-21/{}'.format(file.replace('\n',''))) as logs:
                for line in logs:
                    line = line.split(' ')
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

                        dict_tempt = {'remote_ip': remote_ip, 'ip_client': ip_client, 'time': time, 'domain': domain, 'method': method,
                                      'rote': rote, 'http': http, 'status': status, 'res_size': data,
                                      'link_unknow': link_unknow,
                                      'browser': browser, 'upstream_addr': ip, 'res_duration': time_res}
                        file_json.write(json.dumps(dict_tempt) + '\n');
                else:
                    continue
            file_json.close()
    return True

def convert():
    result = []
    # files = list_file()
    files = list_file_by_quota(quota=5*1024*1024*1024)
    for file in files:
        with open('/data/2021-03-21/data/2021-03-21/{}'.format(file)) as logs:
            for line in logs:
                line = line.split(' ')
                # print (line)
                remote_ip = line[0]
                ip_client = line[1]
                time = line[2].replace('[','').replace(']','')
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

                dict_tempt = {'remote_ip': remote_ip, 'ip_client': ip_client, 'time': time, 'domain': domain, 'method': method,
                              'rote': rote, 'http': http, 'status': status, 'res_size': data, 'link_unknow':link_unknow,
                              'browser': browser, 'upstream_addr': ip, 'res_duration': time_res}
                result.append(dict_tempt)
    file_json = open("result3.json", "a+")
    print(file_json)
    for i in result:
        print(i)
        file_json.write(json.dumps(i) + '\n');
    # Mở file

    # Đóng file

    # Đóng file
    file_json.close()
    return result
                # print (dict_tempt)
                # domain = line[2]
                # if re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}(:\d{1,3})*", domain):  # bỏ qua ip
                #     continue
                # method = line[3].replace('"', '')
                # path_url = line[4]
                # time_ = line[1].replace('[', '').replace(']', '')
                #
                # if 'download' in path_url and method == 'GET':
                #     if '/download' in path_url: # download qua liên kết
                #         file = path_url.split('?')[0].replace('/download','')
                #     if '?download' in path_url: # download trực tiếp
                #         file = path_url.split('?')[0]
                #     if domain not in enumerate.keys():
                #         enumerate[domain] = {file: [1, time_, time_]}
                #     else:
                #         if file not in enumerate[domain].keys():
                #             enumerate[domain][file] = [1, time_, time_]
                #         else:
                #             enumerate[domain][file] = [enumerate[domain].get(file)[0] + 1, enumerate[domain].get(file)[1], time_]

if __name__ == '__main__':
    list_file()
    # get_file_size('diaoc-126-rq.log')
    # list_file_notnull = file_not_null()
    # list_file_10 = list_file_by_quota(quota=20*1024*1024*1024)
    # print(len(list_file_notnull))
    # print(len(list_file_10))
    # convert()
    convert_file()