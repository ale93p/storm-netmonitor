import requests
import json
import time
import argparse
from tcpprobe import ProbeParser
from tcpprobe import ProbeAggregator
 
serverAddress = "127.0.0.1"
serverPort = "5000"

# stormConfigurations
# TODO: get them from storm configuration file
# stormSlots = range(6700,6710)
stormSlots = [5002,5003]

def networkInsert(ts, sh, sp, dh, dp, pk, by):
    url = "http://" + serverAddress + ":" + serverPort + "/api/v0.1/network/insert"
    return requests.get(url + "?ts=" + str(ts) + "&src_host=" + str(sh) + "&src_port=" + str(sp) + "&dst_host=" + str(dh) + "&dst_port=" + str(dp) + "&pkts=" + str(pk) + "&bytes=" + str(by))

def readTcpProbe(file):
    file.seek(0,2)
    while True:
        line = file.readline()
        if not line:
            time.sleep(0.1)
            continue
        yield line

if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter, description="Start netmonitor client")
    parser.add_argument("server_addr", nargs=1, type=str, help="specify server IP address")
    parser.add_argument("-p", "--port", dest="server_port", help="specify server listening port", default="5000")
    args = parser.parse_args()

    serverAddress = args.server_addr
    if args.server_port:
        serverPort = args.server_port
    
    trace = {}
    start_interval = time.time()

    tcpProbeFile = open("/proc/net/tcpprobe","r")
    tcpprobe = readTcpProbe(tcpProbeFile)

    for probe in tcpprobe:
        
        p = ProbeParser(probe)
        if p.sp in stormSlots or p.dp in stormSlots:
            if (p.sh, p.sp, p.dh, p.dp) not in trace:
                trace[p.sh, p.sp, p.dh, p.dp] = ProbeAggregator()
                trace[p.sh, p.sp, p.dh, p.dp].addPacket(int(p.by))
            else:
                trace[p.sh, p.sp, p.dh, p.dp].addPacket(int(p.by))
        
        time_now = time.time()
        if (time_now - start_interval >= 10):
            start_interval = time_now
            for key in trace:
                networkInsert(time_now, key[0], key[1], key[2], key[3], t[key].pkts, t[key].bytes)
                t[key].reset()
        