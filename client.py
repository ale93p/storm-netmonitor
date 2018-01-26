import requests
import json
import time
import argparse
from tcpprobe import ProbeParser
from tcpprobe import ProbeAggregator

# stormConfigurations
# TODO: get them from storm configuration file
# stormSlots = range(6700,6710)
stormSlots = [5001,5002,5003]

def networkInsert(ts, sh, sp, dh, dp, pk, by):
    print(serverAddress)
    print(serverPort)
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
    parser.add_argument("--debug", dest="debug", help="verbose mode", action="store_true", default=False)
    args = parser.parse_args()

    print(" * Running in DEBUG mode * ") if args.debug else None

    serverAddress = args.server_addr
    serverPort = args.server_port if args.server_port else "5000"
    
    trace = {}
    start_interval = None
    init_interval = True

    tcpProbeFile = open("/proc/net/tcpprobe","r")
    tcpprobe = readTcpProbe(tcpProbeFile)

    for probe in tcpprobe:
        
        p = ProbeParser(probe)
        if p.sp in stormSlots or p.dp in stormSlots:
            # print("[DEBUG] ", probe) if args.debug else None
            
            if (p.sh, p.sp, p.dh, p.dp) not in trace:
                trace[p.sh, p.sp, p.dh, p.dp] = ProbeAggregator()
                trace[p.sh, p.sp, p.dh, p.dp].addPacket(int(p.by))
            else:
                trace[p.sh, p.sp, p.dh, p.dp].addPacket(int(p.by))
        
        if len(trace) >= 1 and init_interval: 
            start_interval = time.time()
            init_interval = not init_interval

        if not init_interval and time.time() - start_interval >= 10:
            start_interval = time.time()
            print("[DEBUG] Sending data at ", start_interval) if args.debug else None
            for key in trace:
                res = networkInsert(start_interval, key[0], key[1], key[2], key[3], trace[key].pkts, trace[key].bytes)
                print("[DEBUG] Server response:",res) if args.debug else None
                t[key].reset()
        