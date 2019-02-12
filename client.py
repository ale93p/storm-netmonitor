#!/usr/bin/env python3

import requests, yaml, json, time, argparse, subprocess
from pathlib import Path
from sys import stdout

import threading as thread
import socket, xmlrpc.client

from modules.network_tools import ConntrackParser, ProbeAggregator
from modules.database import *

producer = None
encodingMethod = 'ascii'
networkTopicName = 'netmonitor_network'
portTopicName = 'netmonitor_port'

localhost = ['127.0.0.1', '127.0.1.1']

portMapping = {}
port_init = False

stormSlots = []
zkPort = None

schema_dir = 'database/schema.sql'
netLock = thread.RLock()
portLock = thread.RLock()

def networkInsertDB(now, trace):
    payload = {}
    for key in trace:
        if key[3] in stormSlots + [zkPort] or key[1] == zkPort:
            payload[str(key)] = ",".join([str(trace[key].bytes), str(trace[key].pkts)])
    # print('length payload:', len(payload))
    payload["ts"] = now
    payload["client"] = this_client()
    
    return payload

def generatePortPayload(trace):
    global myIp
    
    payload = {}
    already_done = []
    start = time.time()
    port = ''
    pid = ''
    for key in trace:
        dh = key[2] 
        # if dh in localhost + [myIp]:
        #     port = key[3]
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
        print('this_client',this_client(),flush=True)
        payload["client"] = this_client()
        # p = json.dumps(payload)
        # producer.send(portTopicName, p.encode(encodingMethod))
        return payload

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
    
    if payload: 
        print('this_client',this_client())
        payload["client"] = this_client()
        # p = json.dumps(payload)
        # producer.send(portTopicName, p.encode(encodingMethod))
    return payload

# def readTcpProbe(file):
#     file.seek(0,2)
#     while True:
#         line = file.readline()
#         if not line:
#             time.sleep(0.1)
#             continue
#         yield line

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
    return socket.gethostbyname(this_client())

def writeData(trace):
    global myIp
    global port_init, stormSlots
    now = time.time()
    
    
    if not port_init:
        initPortPayload = initializePortMappingDB(stormSlots)
        if initPortPayload:
            portLock.acquire()
            try:
                portInsert(initPortPayload)
            except:
                print('ERROR [init]:', initPortPayload)
            portLock.release()

    portPayload = portInsertDB(trace)
    if portPayload:
        portLock.acquire()
        try:
            portInsert(portPayload)
        except:       
            print('ERROR:',portPayload)
        portLock.release()

    netPayload = networkInsertDB(now, trace);
    netLock.acquire()
    try:
        networkInsert(netPayload)
    except:
        print('ERROR:', netPayload)
    netLock.release()

def this_client():
    return socket.gethostname()
if __name__ == "__main__":
    global myIp

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
    
    # init_interval = True
    
    db = db_connect()
    init_db(db, schema_dir)
    # producer = KafkaProducer(bootstrap_servers=kafkaServerAddress)

    print(args.storm_conf)
    stormSlots = getStormSlots(args.storm_conf)
    print(stormSlots)
    t = thread.Thread(target = initializePortMappingDB, args = (stormSlots,))
    t.start()
    t.join()

    with xmlrpc.client.ServerProxy("http://{}:{}/".format(serverAddress, serverPort)) as proxy:
        while(True):
            print("Trying to connect to {}:{}... ".format(serverAddress, serverPort), end='')
            stdout.flush()
            try:
                proxy.start(this_client())
                break
            except Exception as e:
                print("FAILED")
                stdout.flush()
                time.sleep(2)
    print("SUCCESS")
    stdout.flush()
            

    # tcpProbeFile = open("/proc/net/tcpprobe","r")
    # tcpprobe = readTcpProbe(tcpProbeFile)


    # for probe in tcpprobe:
    ports_to_filter = stormSlots + [zkPort]
    p = ConntrackParser(localhost + [myIp], ports_to_filter)
    # print(p.ports)

    start_interval = time.time()

    while True:
        
        # p = ProbeParser(probe)
        
        now = time.time()

        if now - start_interval >= 10:

            start_interval = now
            print("[DEBUG] Sending data at ", start_interval) if args.debug else None

            # thread.start_new_thread(sendData, (trace,))
            conntrackCommand = "conntrack -L -z"
            conntrack_output = p.parseOutput(subprocess.check_output(conntrackCommand.split())) 

            # print(conntrack_output)
            if conntrack_output:
                # print('length conntrack:', len(conntrack_output))

                trace = {}
                for connection in conntrack_output:
                    ### FILTER BY INCOMING PACKETS ?
                    #connection['dst'] == myIp and 
                    if 'packets' in connection and int(connection['packets']) > 0:
                        trace[connection['src'],int(connection['sport']),connection['dst'],int(connection['dport'])] = ProbeAggregator(pkts = connection['packets'], bts = connection['bytes'])
                
                # print('length trace:', len(trace))
                t = thread.Thread(target = writeData, args = (trace,))    
                t.start()

            with xmlrpc.client.ServerProxy("http://{}:{}/".format(serverAddress, serverPort)) as proxy:
                if(proxy.is_test_finished()):
                    break
        
        

### Understand that test is finished

print("Waiting threads to finish")
start_waiting = time.time()
active_threads = thread.active_count()
while(active_threads > 1):
    time.sleep(2)
    active_threads_now = thread.active_count()
    if active_threads_now != active_threads:
        active_threads = active_threads_now
        start_waiting = time.time()

        print("Active threads: {}".format(active_threads - 1))

    if time.time() - start_waiting > 60:
        print("[ERROR] Reached timeout. The threads won't be waited")
        break


print("Start dumping file")
fileDump = "database/" + this_client() + "_dump.sql"

db_dump(fileDump)
db.close()

with xmlrpc.client.ServerProxy("http://{}:{}/".format(serverAddress, serverPort)) as proxy:
    with open(fileDump, "rb") as handle:
        binary_data = xmlrpc.client.Binary(handle.read())
    proxy.end(this_client(), binary_data)

print("finished execution")