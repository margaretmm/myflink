import time

import sys
from prometheus_client import  Gauge, start_http_server
import subprocess
import re
import logging
import json
import http.client
import os

check_disk = Gauge("disk_usage", "A gauge")
success_rate = Gauge("success_rate", "success_rate value of the request")
'''jvm info'''
gc_ps_scavenge_time = Gauge("gc_ps_scavenge_time", "gc.ps_scavenge.time")
gc_ps_marksweep_count = Gauge("gc_ps_marksweep_count", "gc.ps_marksweep.count")
gc_ps_marksweep_time = Gauge("gc_ps_marksweep_time", "gc.ps_marksweep.time")
gc_ps_scavenge_count = Gauge("gc_ps_scavenge_count", "gc.ps_scavenge.count")

mem = Gauge("mem", "mem")
mem_free = Gauge("mem_free", "mem.free")

heap = Gauge("heap", "heap")
heap_init = Gauge("heap_init", "heap.init")
heap_used = Gauge("heap_used", "heap.used")
heap_committed = Gauge("heap_committed", "heap.committed")

heap_committed = Gauge("nonheap", "nonheap")
nonheap_init = Gauge("nonheap_init", "nonheap.init")
nonheap_used = Gauge("nonheap_used", "nonheap.used")
nonheap_committed = Gauge("nonheap_committed", "nonheap.committed")


hn = os.getenv('HOSTNAME')
log_file="/data/log/"+hn+"/saasComponentGateway.log"#saasComponentGateway
jvm_info="{\"mem\":1105312,\"mem.free\":800148,\"processors\":24,\"instance.uptime\":652914,\"uptime\":751938,\"systemload.average\":0.73,\"heap.committed\":991744,\"heap.init\":1048576,\"heap.used\":191595,\"heap\":991744,\"nonheap.committed\":115416,\"nonheap.init\":2496,\"nonheap.used\":113568,\"nonheap\":0,\"threads.peak\":86,\"threads.daemon\":85,\"threads.totalStarted\":99,\"threads\":86,\"classes\":13110,\"classes.loaded\":13110,\"classes.unloaded\":0,\"gc.ps_scavenge.count\":29,\"gc.ps_scavenge.time\":3466,\"gc.ps_marksweep.count\":3,\"gc.ps_marksweep.time\":5716}"
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def http_conn(ip="localhost",port=8080,timeout=60):
    try:
        conn = http.client.HTTPConnection(ip, port, timeout=timeout)
        conn.request("GET", "/metrics")
        r1 = conn.getresponse()
        msg = r1.read().decode()
        # print(msg)
        jvm_json = json.loads(msg)
        return jvm_json,jvm_json.keys()
    except Exception as e:
        #raise e
        logger.error("HTTPConnection fail!!")
        return None,[]

def get_jvm_info(ip="localhost",port=8080):
    lst=[]
    jvm_json=None
    while None==jvm_json:
        jvm_json, lst = http_conn(ip, port, timeout=30)

    #logger.info(lst)
    if "gc.ps_scavenge.time" in lst:
        gc_ps_scavenge_time.set(jvm_json["gc.ps_scavenge.time"])
    else:
        gc_ps_scavenge_time.set(0)
        logger.info("has no key:gc.ps_scavenge.time !")

    if "gc.ps_marksweep.count" in lst:
        gc_ps_marksweep_count.set(jvm_json["gc.ps_marksweep.count"])
    else:
        gc_ps_marksweep_count.set(0)
        logger.info("has no key:gc.ps_marksweep.count !")

    if "gc.ps_marksweep.time" in lst:
        gc_ps_marksweep_time.set(jvm_json["gc.ps_marksweep.time"])
    else:
        gc_ps_marksweep_time.set(0)
        logger.info("has no key:gc.ps_marksweep.time !")

    if "gc.ps_scavenge.count" in lst:
        gc_ps_scavenge_count.set(jvm_json["gc.ps_scavenge.count"])
    else:
        gc_ps_scavenge_count.set(0)
        logger.info("has no key:gc.ps_scavenge.count !")

    if "mem" in lst:
        mem.set(jvm_json["mem"])
    else:
        mem.set(0)
        logger.info("has no key:mem !")

    if "mem.free" in lst:
        mem_free.set(jvm_json["mem.free"])
    else:
        mem_free.set(0)
        logger.info("has no key:mem.free !")

    if "heap" in lst:
        heap.set(jvm_json["heap"])
    else:
        heap.set(0)
        logger.info("has no key:heap !")

    if "heap.init" in lst:
        heap_init.set(jvm_json["heap.init"])
    else:
        heap_init.set(0)
        logger.info("has no key:heap.init !")

    if "heap.used" in lst:
        heap_used.set(jvm_json["heap.used"])
    else:
        heap_used.set(0)
        logger.info("has no key:heap.used !")

    if "heap.committed" in lst:
        heap_committed.set(jvm_json["heap.committed"])
    else:
        heap_committed.set(0)
        logger.info("has no key:heap.committed !")

    if "nonheap.init" in lst:
        nonheap_init.set(jvm_json["nonheap.init"])
    else:
        nonheap_init.set(0)
        logger.info("has no key:nonheap.init !")

    if "nonheap.used" in lst:
        nonheap_used.set(jvm_json["nonheap.used"])
    else:
        nonheap_used.set(0)
        logger.info("has no key:nonheap.used !")

    if "nonheap.committed" in lst:
        nonheap_committed.set(jvm_json["nonheap.committed"])
    else:
        nonheap_committed.set(0)
        logger.info("has no key:nonheap.committed !")


def get_disk_usage():
    a= subprocess.check_output(["du -sh /data"],shell=True).decode()
    b= re.findall('(^\d+(\.\d+)?)',a).pop(0)
    #print(a, float(b[0]))
    check_disk.set(float(b[0]))

def get_log_info():
    code=load_file(log_file)
    success_rate.set(code)


def load_file(file_name):
    #f = open(file_name)  # 打开训练数据集所在的文档
    try:
        with open(file_name, "r", encoding="utf-8") as f:
            for row in f.readlines():
                #print(row)
                s= row.split(" ")
                sj=json.loads(s[-1])
                print(sj["r"])
    except  ValueError as e:
        logger.info(e)
        #print("file handler exception!!!!!")
        #logger.info('@@@file handler exception!!!!!')
    except OSError as err:
        logger.info("OS error: {0}".format(err))
    except:
        logger.info("Unexpected error:", sys.exc_info()[0])
    finally:
        logger.info('###requests='+str(sj["r"]))
    return sj["r"]

if __name__ == '__main__':
    #load_file("./saasComponentGateway.log")
    start_http_server(5000)
    while True:
        time.sleep(5)
        get_disk_usage()
        get_log_info()
        get_jvm_info()
