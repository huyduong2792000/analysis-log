# import re

# conf = '$remote_addr - $remote_user [$time_local] "$request" $status $body_bytes_sent "$http_referer" "$http_user_agent"'
# regex = ''.join(
#     '(?P<' + g + '>.*?)' if g else re.escape(c)
#     for g, c in re.findall(r'\$(\w+)|(.)', conf))
# log = '112.3.194.120 - - [17/Jan/2015:20:07:34 +0800] "GET /Introdction%20to%20Guitar/1%20-%202%20-%20Choosing%20the%20Right%20Guitar-%20Right-Handed%20vs%20Left-Handed%20(3-20).mp4 HTTP/1.1" 206 546849 "http://example.com/video/302/" "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36"'

# m = re.match(regex, log)

# print(m.groupdict())
import re
conf = '$remote_ip $ip_client [$time] $domain "$request" $status $res_size "$http_referer" "$http_user_agent" - $unkown $upstream_addr $res_time -'
log = '192.168.241.119 14.169.52.225 [2021-03-21T23:58:54+07:00] cafebiz.vn "GET /ong-trum-kin-tieng-thi-cong-thiet-ke-cho-hang-loat-chuoi-the-coffee-house-bach-hoa-xanh-fpt-vingroup-flc-moi-3-nam-tuoi-doanh-thu-vai-tram-ty-dong-nam-20210313224854838.chn HTTP/1.1" 200 156270 "-" "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; MDDC)" - 695 192.168.241.108:7695 0.002 -'
regex = ''.join(
    '(?P<' + g + '>.*?)' if g else re.escape(c)
    for g, c in re.findall(r'\$(\w+)|(.)', conf))
m = re.match(regex, log)
print(m.groupdict())