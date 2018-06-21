#!/usr/bin/env python3

import requests
import yaml
import json
import time
import argparse
# import psutil
import subprocess
import _thread as thread
from pathlib import Path
from modules.tcpprobe import ProbeParser, ProbeAggregator
import socket

from kafka import KafkaProducer
from kafka.errors import KafkaError

producer = None
encodingMethod = 'ascii'
networkTopicName = 'netmonitor_network'
portTopicName = 'netmonitor_port'

localhost = ['127.0.0.1', '127.0.1.1'] 
portMapping = {}
port_init = False

stormSlots = []
zkPort = None

# def networkInsertFull(now, trace):
#     url = "http://" + serverAddress + ":" + serverPort + "/api/v0.2/network/insert"
#     # return requests.get(url + "?ts=" + str(ts) + "&src_host=" + str(sh) + "&src_port=" + str(sp) + "&dst_host=" + str(dh) + "&dst_port=" + str(dp) + "&pkts=" + str(pk) + "&bytes=" + str(by))
#     payload = {}
#     for key in trace:
#         if key[3] in stormSlots + [zkPort] or key[1] == zkPort:
#             payload[key] = ",".join([str(trace[key].bytes), str(trace[key].pkts)])
#     payload["ts"] = now
#     return requests.post(url, data = payload)

def networkInsertKafka(now, trace):
    payload = {}
    for key in trace:
        if key[3] in stormSlots + [zkPort] or key[1] == zkPort:
            payload[key] = ",".join([str(trace[key].bytes), str(trace[key].pkts)])
    payload["ts"] = now
    payload["client"] = socket.gethostname()

    newPayload = {}
    for key in payload:
        newPayload[str(key)] = payload[key]

    p = json.dumps(newPayload)
    producer.send(networkTopicName, p.encode(encodingMethod))

def generatePortPayload(trace):
    global myIp
    payload = {}
    already_done = []
    start = time.time()
    port = ''
    pid = ''
    for key in trace:
        sh = key[0]
        # sp = key[1]
        dh = key[2] 
        # dp = key[3]
        if sh in localhost + myIp:
            port = key[1]
        elif dh in localhost + myIp:
            port = key[3]
        
        if port not in already_done:
            already_done.append(port)
            pid = getPidByPort(port)
            print(port,pid)
            if pid:
                if port not in portMapping or portMapping[port] != pid:
                # sobstitute the old pid with the new one (temporary solution)  
                    portMapping[port] = pid
                    payload[port] = pid

            
    # print("length",len(trace),"payload in", time.time() - start)
    return payload

# def portInsertFull(trace):
#     url = "http://" + serverAddress + ":" + serverPort + "/api/v0.2/port/insert"
#     payload = generatePortPayload(trace)
#     if payload: return requests.post(url, data = payload)

def portInsertKafka(trace):
    payload = generatePortPayload(trace)
    if payload:
        payload["client"] = socket.gethostname()
        p = json.dumps(payload)
        producer.send(portTopicName, p.encode(encodingMethod))

# def initializePortMappingFull(ports):
#     global port_init
#     url = "http://" + serverAddress + ":" + serverPort + "/api/v0.2/port/insert"
#     payload = {}
#     for port in ports:
#         if port not in portMapping:
#             pid = getPidByPort(port)
#             if pid:
#                 portMapping[port] = pid
#                 payload[port] = pid
#                 port_init = True
    
#     if port_init: return requests.post(url, data = payload)

def initializePortMappingKafka(ports):
    global port_init
    payload = {}
    for port in ports:
        if port not in portMapping:
            pid = getPidByPort(port)
            if pid:
                portMapping[port] = pid
                payload[port] = pid
                port_init = True
    
    if port_init:
        payload["client"] = socket.gethostname()
        p = json.dumps(payload)
        producer.send(portTopicName, p.encode(encodingMethod))

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
    # for p in psutil.net_connections('tcp'):
    #     if p.laddr and str(p.laddr.port) == str(port):
    #         return p.pid
    # cmd = 'lsof -n -i :' + str(port)
    cmd = "ss -ptn sport = :" + str(port)
    proc = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
    out, err = proc.communicate()
    try:
        pid = str(out)[2:].split('\\n')[1].split("pid=")[1].split(",fd")[0]
        return pid
    except:
        return None
     
        
def getMyIp():
    method_1 = requests.get('https://api.ipify.org/?format=json').json()['ip']
    method_2 = socket.gethostbyname(socket.gethostname())
    return [method_1, method_2]

def sendData(trace):
    global myIp
    global port_init, stormSlots
    now = time.time()
    
    print('[DEBUG] newT:',now) if args.debug else None
    #for key in trace:
        #if key[3] in stormSlots:
            # res = networkInsert(now, key[0], key[1], key[2], key[3], trace[key].pkts, trace[key].bytes)
    res = networkInsertKafka(now, trace);
    print('[DEBUG] send:',time.time()) if args.debug else None
    print("[DEBUG] Network Insert:",res) if args.debug else None
        #
    if not port_init: initializePortMappingKafka(stormSlots)
    #    res = portInsert(key[0],key[1],key[2],key[3])
    res = portInsertKafka(trace)
    #    print("[DEBUG] Port Insert:",res) if args.debug else None

if __name__ == "__main__":
    global storm_slots, myIp

    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter, description="Start netmonitor client")
    parser.add_argument("server_addr", nargs=1, type=str, help="specify server IP address")
    parser.add_argument("-p", "--port", dest="server_port", help="specify server listening port")
    parser.add_argument("-k", "--kafka", dest="kafka_address", help="specify kafka server address")
    parser.add_argument("-c", "--storm-conf", dest="storm_conf", help="specify storm configuration file path", default=str(Path.home())+'/apache-storm-1.2.1/conf/storm.yaml')
    parser.add_argument("--zookeeper", dest="zookeeper", help="analyse also zookeeper connections")
    parser.add_argument("--debug", dest="debug", help="verbose mode", action="store_true", default=False)
    args = parser.parse_args()

    print(" * Running in DEBUG mode * ") if args.debug else None
    
    print('[DEBUG] strt:',time.time()) if args.debug else None
    serverAddress = args.server_addr[0]
    if args.kafka_address: kafkaServerAddress = args.kafka_address
    else: kafkaServerAddress = serverAddress
    serverPort = args.server_port if args.server_port else '5000'
    if args.zookeeper: zkPort = int(args.zookeeper)
    
    myIp = getMyIp()
    
    trace = {}
    start_interval = None
    init_interval = True

    producer = KafkaProducer(bootstrap_servers=kafkaServerAddress)

    stormSlots = getStormSlots(args.storm_conf)
    thread.start_new_thread(initializePortMappingKafka, (stormSlots,))
    
    tcpProbeFile = open("/proc/net/tcpprobe","r")
    tcpprobe = readTcpProbe(tcpProbeFile)
    
    for probe in tcpprobe:
        
        p = ProbeParser(probe)
        if p.sp in stormSlots + [zkPort] or p.dp in stormSlots + [zkPort]: #ZooKeeper:
        # Filter out ACK direction
        #if p.dp in stormSlots:    
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