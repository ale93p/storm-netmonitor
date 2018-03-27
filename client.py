import requests
import yaml
import json
import time
import argparse
import psutil
import _thread as thread
from pathlib import Path
from modules.tcpprobe import ProbeParser, ProbeAggregator

localhost = ['127.0.0.1', '127.0.1.1'] 
portMapping = {}
port_init = False

stormSlots = []
zkPort = '2181'

def networkInsertFull(now, trace):
    url = "http://" + serverAddress + ":" + serverPort + "/api/v0.2/network/insert"
    # return requests.get(url + "?ts=" + str(ts) + "&src_host=" + str(sh) + "&src_port=" + str(sp) + "&dst_host=" + str(dh) + "&dst_port=" + str(dp) + "&pkts=" + str(pk) + "&bytes=" + str(by))
    payload = {}
    for key in trace:
        # if key[3] in stormSlots + [zkPort] or key[1] == zkPort: 
        payload[key] = ",".join([str(trace[key].pkts), str(trace[key].pkts)])
    payload["ts"] = now
    return requests.post(url, data = payload)

def networkInsert(ts, sh, sp, dh, dp, pk, by):  
    """ deprecate """
    #print('conn:',sh,sp,dh,dp,'pkts:',pk,'data:',by)
    url = "http://" + serverAddress + ":" + serverPort + "/api/v0.1/network/insert"
    return requests.get(url + "?ts=" + str(ts) + "&src_host=" + str(sh) + "&src_port=" + str(sp) + "&dst_host=" + str(dh) + "&dst_port=" + str(dp) + "&pkts=" + str(pk) + "&bytes=" + str(by))

def generatePortPayload(trace):
    payload = {}
    for key in trace:
        port = ''
        pid = ''
        sh = key[0]
        sp = key[1]
        dh = key[2]
        dp = key[3]
        if sh == myIp or sh in localhost: port = sp
        elif dh == myIp or dh in localhost: port = dp
        pid = getPidByPort(port)
        if pid:
            if port not in portMapping or portMapping[port] != pid:
            # sobstitute the old pid with the new one (temporary solution)  
                portMapping[port] = pid
                payload[port] = pid
    return payload

def portInsertFull(trace):
    global myIp
    url = "http://" + serverAddress + ":" + serverPort + "/api/v0.2/port/insert"
    payload = generatePortPayload(trace)
    return requests.post(url, data = payload)

def portInsert(sh, sp, dh, dp):
    """ deprecate """
    global myIp
    url = "http://" + serverAddress + ":" + serverPort + "/api/v0.1/port/insert"
    port = ''
    pid = ''
    if sh == myIp or sh in localhost: port = sp
    elif dh == myIp or dh in localhost: port = dp
    
    pid = getPidByPort(port)
    if pid:
        if port not in portMapping or portMapping[port] != pid:
            # sobstitute the old pid with the new one (temporary solution)  
            portMapping[port] = pid
            return requests.get(url + "?port=" + str(port) + "&pid=" + str(pid))
        else:
            return 'OK'

def initializePortMappingFull(ports):
    global port_init
    url = "http://" + serverAddress + ":" + serverPort + "/api/v0.2/port/insert"
    payload = {}
    for port in ports:
        if port not in portMapping:
            pid = getPidByPort(port)
            if pid:
                portMapping[port] = pid
                payload[port] = pid

    port_init = True
    return requests.post(url, data = payload)

def initializePortMapping(ports):
    """ deprecate """
    global port_init
    url = "http://" + serverAddress + ":" + serverPort + "/api/v0.1/port/insert"
    for port in ports:
        if port not in portMapping:
            pid = getPidByPort(port)
            if pid:
                portMapping[port] = pid
                requests.get(url + "?port=" + str(port) + "&pid=" + str(pid))
                port_init = True

def readTcpProbe(file):
    file.seek(0,2)
    while True:
        line = file.readline()
        if not line:
            time.sleep(0.1)
            continue
        yield line

def getStormSlots(conf):
    f = open(conf, 'r')
    return yaml.load(f)['supervisor.slots.ports']

def getPidByPort(port):
    for p in psutil.net_connections('tcp'):
        if p.laddr and str(p.laddr.port) == str(port):
            return p.pid
        
def getMyIp():
    return requests.get('https://api.ipify.org/?format=json').json()['ip']

def sendData(trace):
    global myIp
    global port_init, stormSlots
    now = time.time()
    
    print('[DEBUG] newT:',now) if args.debug else None
    #for key in trace:
        #if key[3] in stormSlots:
            # res = networkInsert(now, key[0], key[1], key[2], key[3], trace[key].pkts, trace[key].bytes)
    res = networkInsertFull(now, trace);
    print('[DEBUG] send:',time.time()) if args.debug else None
    print("[DEBUG] Network Insert:",res) if args.debug else None
        #
    if not port_init: initializePortMappingFull(stormSlots)
    #    res = portInsert(key[0],key[1],key[2],key[3])
    res = portInsertFull(trace)
    #    print("[DEBUG] Port Insert:",res) if args.debug else None

if __name__ == "__main__":
    global storm_slots, myIp

    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter, description="Start netmonitor client")
    parser.add_argument("server_addr", nargs=1, type=str, help="specify server IP address")
    parser.add_argument("-p", "--port", dest="server_port", help="specify server listening port")
    parser.add_argument("-c", "--storm-conf", dest="storm_conf", help="specify storm configuration file path", default=str(Path.home())+'/apache-storm-1.1.1/conf/storm.yaml')
    parser.add_argument("--debug", dest="debug", help="verbose mode", action="store_true", default=False)
    args = parser.parse_args()

    print(" * Running in DEBUG mode * ") if args.debug else None
    
    print('[DEBUG] strt:',time.time()) if args.debug else None
    serverAddress = args.server_addr[0]
    serverPort = args.server_port[0] if args.server_port else '5000'
    
    myIp = getMyIp()

    trace = {}
    start_interval = None
    init_interval = True

    stormSlots = getStormSlots(args.storm_conf)
    thread.start_new_thread(initializePortMappingFull, (stormSlots,))
    
    tcpProbeFile = open("/proc/net/tcpprobe","r")
    tcpprobe = readTcpProbe(tcpProbeFile)
    
    for probe in tcpprobe:
        
        p = ProbeParser(probe)
        if p.sp in stormSlots or p.dp in stormSlots:
        # Filter out ACK direction
        #if p.dp in stormSlots:    
            if (p.sh, p.sp, p.dh, p.dp) not in trace:
                trace[p.sh, p.sp, p.dh, p.dp] = ProbeAggregator()
                trace[p.sh, p.sp, p.dh, p.dp].addPacket(int(p.by))
            else:
                trace[p.sh, p.sp, p.dh, p.dp].addPacket(int(p.by))
        elif p.sp == zkPort or p.dp == zkPort: #ZooKeeper
            if (p.sh, p.sp, p.dh, p.dp) not in trace:
                trace[p.sh, p.sp, p.dh, p.dp] = ProbeAggregator()
                trace[p.sh, p.sp, p.dh, p.dp].addPacket(int(p.by))
            else:
                trace[p.sh, p.sp, p.dh, p.dp].addPacket(int(p.by))
        
        now = time.time()
        if init_interval:
            if len(trace) >= 1: 
                start_interval = now
                print('[DEBUG] frst:',start_interval, time.time()) if args.debug else None
                init_interval = not init_interval
	
        elif now - start_interval >= 10:
                start_interval = now
                print("[DEBUG] Sending data at ", start_interval) if args.debug else None

                thread.start_new_thread(sendData, (trace,))    
                
                trace = {} 