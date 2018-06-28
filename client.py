#!/usr/bin/env python3

import requests, yaml, json, time, argparse, subprocess
from pathlib import Path
from sys import stdout

import threading as thread
import socket, xmlrpc.client

from modules.tcpprobe import ProbeParser, ProbeAggregator
from modules.database import *

producer = None
encodingMethod = 'ascii'
networkTopicName = 'netmonitor_network'
portTopicName = 'netmonitor_port'

localhost = ['127.0.0.1', '127.0.1.1']
this_client = socket.gethostname()
portMapping = {}
port_init = False

stormSlots = []
zkPort = None

schema_dir = 'database/schema.sql'
lock = thread.RLock()

def networkInsertDB(now, trace):
    payload = {}
    for key in trace:
        if key[3] in stormSlots + [zkPort] or key[1] == zkPort:
            payload[str(key)] = ",".join([str(trace[key].bytes), str(trace[key].pkts)])
    payload["ts"] = now
    payload["client"] = this_client
    
    lock.acquire()
    networkInsert(payload)
    lock.release()

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
        if sh in localhost + [myIp]:
            port = key[1]
        elif dh in localhost + [myIp]:
            port = key[3]
        
        if port not in already_done:
            already_done.append(port)
            pid = getPidByPort(port)
            # print(port,pid)
            if pid:
                if port not in portMapping or portMapping[port] != pid:
                # sobstitute the old pid with the new one (temporary solution)  
                    portMapping[port] = pid
                    payload[port] = pid

            
    # print("length",len(trace),"payload in", time.time() - start)
    return payload

def portInsertDB(trace):
    payload = generatePortPayload(trace)
    if payload:
        payload["client"] = this_client
        # p = json.dumps(payload)
        # producer.send(portTopicName, p.encode(encodingMethod))
        lock.acquire()
        portInsert(payload)
        lock.release()


def initializePortMappingDB(ports):
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
        payload["client"] = this_client
        # p = json.dumps(payload)
        # producer.send(portTopicName, p.encode(encodingMethod))
        lock.acquire()
        portInsert(payload)
        lock.release()

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

    cmd = "ss -ptn sport = :" + str(port)
    proc = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
    out, err = proc.communicate()
    try:
        pid = str(out)[2:].split('\\n')[1].split("pid=")[1].split(",fd")[0]
        return pid
    except:
        return None
        
def getMyIp():
    return socket.gethostbyname(this_client)

def writeData(trace):
    global myIp
    global port_init, stormSlots
    now = time.time()
    
    print('[DEBUG] newT:',now) if args.debug else None

    networkInsertDB(now, trace);
    print('[DEBUG] send:',time.time()) if args.debug else None
    print("[DEBUG] Network Insert:",res) if args.debug else None

    if not port_init: initializePortMappingDB(stormSlots)

    portInsertDB(trace)


if __name__ == "__main__":
    global storm_slots, myIp

    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter, description="Start netmonitor client")
    parser.add_argument("server_addr", nargs=1, type=str, help="specify server IP address")
    parser.add_argument("-p", "--port", dest="server_port", help="specify server listening port")
    # parser.add_argument("-k", "--kafka", dest="kafka_address", help="specify kafka server address")
    parser.add_argument("-c", "--storm-conf", dest="storm_conf", help="specify storm configuration file path", default=str(Path.home())+'/apache-storm-1.2.1/conf/storm.yaml')
    parser.add_argument("--zookeeper", dest="zookeeper", help="analyse also zookeeper connections")
    parser.add_argument("--debug", dest="debug", help="verbose mode", action="store_true", default=False)
    args = parser.parse_args()

    print(" * Running in DEBUG mode * ") if args.debug else None
    
    print('[DEBUG] strt:',time.time()) if args.debug else None
    serverAddress = args.server_addr[0]
    # if args.kafka_address: kafkaServerAddress = args.kafka_address
    # else: kafkaServerAddress = serverAddress
    serverPort = args.server_port if args.server_port else '8585'
    if args.zookeeper: zkPort = int(args.zookeeper)
    
    myIp = getMyIp()
    
    trace = {}
    
    init_interval = True
    
    db = db_connect()
    init_db(db, schema_dir)
    # producer = KafkaProducer(bootstrap_servers=kafkaServerAddress)

    stormSlots = getStormSlots(args.storm_conf)
    t = thread.Thread(target = initializePortMappingDB, args = (stormSlots,))
    t.start()
    t.join()

    with xmlrpc.client.ServerProxy("http://{}:{}/".format(serverAddress, serverPort)) as proxy:
        while(True):
            print("Trying to connect to {}:{}... ".format(serverAddress, serverPort), end='')
            stdout.flush()
            try:
                proxy.start(this_client)
                break
            except Exception as e:
                print("FAILED")
                stdout.flush()
                time.sleep(2)
    print("SUCCESS")
    stdout.flush()   
            

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

                # thread.start_new_thread(sendData, (trace,))    
                t = thread.Thread(target = writeData, args = (trace,))    
                t.start()

                trace = {}

                with xmlrpc.client.ServerProxy("http://{}:{}/".format(serverAddress, serverPort)) as proxy:
                    if(proxy.is_test_finished()):
                        break
        
        

### Understand that test is finished

start_waiting = time.time()
active_threads = thread.active_count()
while(active_threads > 1):
    time.sleep(2)
    active_threads_now = thread.active_count()
    if active_threads_now != active_threads:
        active_threads = active_threads_now
        start_waiting = time.time()

    if time.time() - start_waiting > 60:
        print("[ERROR] Reached timeout. The threads won't be waited")
        break


fileDump = "database/" + this_client + "_dump.sql"

db_dump(fileDump)
db.close()

with xmlrpc.client.ServerProxy("http://{}:{}/".format(serverAddress, serverPort)) as proxy:
    with open(fileDump, "rb") as handle:
        binary_data = xmlrpc.client.Binary(handle.read())
    proxy.end(this_client, binary_data)