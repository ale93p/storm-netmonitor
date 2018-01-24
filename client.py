import requests
import json
import time
import argparse

serverAddress = "127.0.0.1"
serverPort = "5000"

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
    if options.server_port:
        serverPort = args.server_port

    tcpProbeFile = open("/proc/net/tcpprobe","r")
    loglines = readTcpProbe(tcpProbeFile)
    for line in loglines:
        print(line)