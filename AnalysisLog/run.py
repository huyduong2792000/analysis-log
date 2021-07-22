import re
from multiprocessing import Process, Manager
import time
import itertools 

conf = '$remote_addr $http_x_forwarded_for [$time_iso8601] $http_host "$request" $status $bytes_sent "$http_referer" "$http_user_agent" $rest_value'
regex = ''.join(
        '(?P<' + g + '>.*)' if g else re.escape(c)
        for g, c in re.findall(r'\$(\w+)|(.)', conf))

def parse_log_to_dict(raw_log = None):
    m = re.match(regex, raw_log)
    if m:
        return m.groupdict()
    return {}

def do_work(in_queue, out_list):
    while True:
        item = in_queue.get()
        line_no, line = item

        # exit signal 
        if line == None:
            return

        # fake work
        # time.sleep(.5)
        result = (line_no, line)
        print("===result===", result)

        out_list.append(result)


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
    with open("/home/vunm/analysis-log/msoha-27-rq.log") as f:
        iters = itertools.chain(f, (None,)*num_workers)
        for num_and_line in enumerate(iters):
            work.put(num_and_line)

    for p in pool:
        p.join()

    # get the results
    # example:  [(1, "foo"), (10, "bar"), (0, "start")]
    print(sorted(results))