import re
from multiprocessing import Process, Manager
import time
import itertools 

TOPIC = 'test-druid5'

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
    if dict_log:
        future = json_producer.send(TOPIC, dict_log)
        json_producer.flush()
def do_work(in_queue, out_list):
    while True:
        item = in_queue.get()
        line = item

        # exit signal 
        if line == None:
            return

        # fake work
        # time.sleep(.5)
        print("===line===", line)
        out_list.append(line)


if __name__ == "__main__":
    num_workers = 4

    manager = Manager()
    results = manager.list()
    work = manager.Queue(num_workers)

    # start for workers    
    pool = []
    for i in range(0, num_workers):
        p = Process(target=do_work, args=(work, results))
        p.start()
        pool.append(p)

    # produce data
    with open("/home/vunm/analysis-log/msoha-27-rq.log") as logs:
        # iters = itertools.chain(f, (None,)*num_workers)
        for line in logs:
            work.put(line)

    for p in pool:
        p.join()

    print(results)