#!/usr/bin/env python3

import argparse, time, datetime
from os.path import join, dirname, abspath, getmtime #, isfile
from sys import stdout

import socket
import threading as thread
from xmlrpc.server import SimpleXMLRPCServer

from modules.stormapi import StormCollector
from modules.database import *

encodingMethod = 'ascii'
networkTopicName = 'netmonitor_network'
portTopicName = 'netmonitor_port'


# lock = threading.RLock()
lock = thread.RLock()
start_time = time.time()
#filename = 'network_db_' + time.strftime("%d%m%y%H%M%s") + '.csv'
schema_dir = 'database/schema.sql'
db = None
localhost = ['127.0.0.1','127.0.1.1'] 


clientsList = []
testFinished = False


storm = StormCollector(None)
# gotFromDb = False

def humansize(n, bytes=True):
    if bytes:
        suffixes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
        div = 1024.
    else:
        suffixes = ['', 'K', 'M', 'B', 'T']
        div = 1000.

    i = 0
    while n >= div and i < len(suffixes)-1:
        n /= div
        i += 1
    f = ('%.2f' % n).rstrip('0').rstrip('.')

    return '%s %s' % (f, suffixes[i])

def reload_storm(): 
    global storm #, gotFromDb

    res = storm.reload()
    if res: 
        lock.acquire()
        updateStormDb(storm)
        lock.release()
    
    if res and len(storm.topologies) > 0: delay = 120.0
    else: delay = 15.0

    thread.Timer(delay, reload_storm).start()
        # gotFromDb = False
      

def is_running():
    global clientsList
    return not len(clientsList) == 0

def start(client):
    global clientsList
    clientsList.append(client)
    print("[INFO] '{}' running".format(client))
    stdout.flush()
    return True

def end(client, arg):
    global clientsList
    
    with open("database/{}_dump.sql".format(client), "wb") as handle:
        handle.write(arg.data)

    print(client, clientsList)
    if len(clientsList) == 1 and client in clientsList:
        global db
        db_dump("database/{}_dump.sql".format(socket.gethostname()))

    clientsList.remove(client)
    print(client, clientsList)
    print("[INFO] '{}' has completed".format(client))
    stdout.flush()
    
    return True
    
def set_test_finished():
    global testFinished
    testFinished = True
    return True

def is_test_finished():
    global testFinished
    return testFinished

def start_xmlrpc_server():
    server = SimpleXMLRPCServer(("0.0.0.0", 8585))
    server.register_function(is_running, "is_running")
    server.register_function(start, "start")
    server.register_function(end, "end")
    server.register_function(is_test_finished, "is_test_finished")
    server.register_function(set_test_finished, "set_test_finished")
    server.serve_forever()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter, description="Start netmonitor server")
    parser.add_argument("-p", "--port", dest="port", type=int, help="specify listening port", default=5000)
    parser.add_argument("-k", "--kafka", dest="kafka_address", help="specify kafka server address")
    parser.add_argument("-v", "--verbose", dest="verbose", help="verbose mode", action="store_true", default=False)
    parser.add_argument("--initdb", dest="initdb", help="initialize the database", action="store_true", default=False)
    # parser.add_argument("-w", dest="csv", help="specify a csv file to store results", type=str) # TODO: write to csv file if selected
    parser.add_argument("--nimbus", dest="nimbus_address", help="storm nimbus address", type=str, default=None)    
    args = parser.parse_args()

    db = db_connect()
    init_db(db, schema_dir) if args.initdb else None

    # if args.kafka_address: kafkaServerAddress = args.kafka_address
    # else: kafkaServerAddress = socket.gethostname()

    if(args.nimbus_address):
        storm = StormCollector(args.nimbus_address)
    else:
        print("You must define nimbus address [--nimbus]")
        quit()
        
    print(" * Enabled verbose output * ") if args.verbose else None

    print("Ctrl+C to kill the application")

    reload_storm()

    start_xmlrpc_server()



    