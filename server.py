#!/usr/bin/env python3

# from flask import Flask, jsonify, request, abort, render_template, g, redirect, url_for 
import argparse
import time, datetime
import sqlite3
from os.path import join, dirname, abspath, getmtime #, isfile
import sys
from modules.stormapi import StormCollector
# import threading
import _thread as thread

from kafka import KafkaConsumer
import json
import socket


encodingMethod = 'ascii'
networkTopicName = 'netmonitor_network'
portTopicName = 'netmonitor_port'


# lock = threading.RLock()
lock = thread.allocate_lock()
start_time = time.time()
#filename = 'network_db_' + time.strftime("%d%m%y%H%M%s") + '.csv'
db_dir = 'database/netmonitor.db'
schema_dir = 'database/schema.sql'
localhost = ['127.0.0.1','127.0.1.1'] 

storm = StormCollector(None)
gotFromDb = False

# app = Flask(__name__)
# app.config.from_object(__name__)
# app.config.update(dict(
#     DATABASE=join(app.root_path, db_dir),
#     SECRET_KEY='development key',
#     USERNAME='admin',
#     PASSWORD='default'
# ))
# app.config.from_envvar('FLASKR_SETTINGS', silent=True)

db_config = dict(
    DATABASE=join(dirname(abspath(__file__)), db_dir),
    SECRET_KEY='development key',
    USERNAME='admin',
    PASSWORD='default'
)

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


### SQLite DBMS functions


def connect_db():
    """Connects to the specific database."""
    # rv = sqlite3.connect(app.config['DATABASE'])
    rv = sqlite3.connect(db_config['DATABASE'], isolation_level = None)
    rv.row_factory = sqlite3.Row
    return rv

def get_db():
    """Opens a new database connection if there is none yet for the
    current application context.
    """
    # if not hasattr(g, 'sqlite_db'):
    #     g.sqlite_db = connect_db()
    # return g.sqlite_db

    return connect_db()

def init_db():
    print("Initilizing db... ", end="")
    lock.acquire()
    db = connect_db()
    with open(schema_dir, mode='r') as f:
        db.cursor().executescript(f.read())
    db.commit()
    db.close()
    lock.release()
    print("OK")


def insert_db(query):
    lock.acquire()
    db = connect()
    cur = db.execute(query)
    db.commit()
    db.close()
    lock.release()
    return cur.lastrowid

def multi_insert_db(listOfQueries):
    lock.acquire()
    db = connect_db()
    for query in listOfQueries:
        db.execute(query)
    db.commit()
    db.close()
    lock.release()

### SQLite Queries

def getSummary():
    db = get_db()
    query = 'SELECT src_addr, src_port, dst_addr, dst_port, SUM(pkts) as pkts, SUM(bytes) as bytes, MAX(ts) as ts \
            FROM connections, probes \
            WHERE connections.ID == probes.connection \
            GROUP BY probes.connection \
            ORDER BY bytes DESC;'
    cur = db.execute(query)
    return cur.fetchall()

def getTopoNetwork(addrs, ports):#, time):
    db = get_db()
    query = 'SELECT client, src_addr, src_port, dst_addr, dst_port, SUM(pkts) as pkts, SUM(bytes) as bytes, MAX(ts) \
            FROM connections, probes \
            WHERE \
            connections.ID == probes.connection \
            AND \
            (\
                (src_addr IN ' + str(addrs) + ' AND src_port IN ' + str(ports) + ') \
                OR \
                (dst_addr IN ' + str(addrs) + ' AND dst_port IN ' + str(ports) + ')\
            ) \
            GROUP BY probes.connection \
            ORDER BY bytes DESC'

        #and ts > ' + str(time) + '\
    cur = db.execute(query)
    return cur.fetchall()

def getWorkerDataIn(addr, ports):#, time):

    if len(ports) == 1: ports = "('" + str(ports[0]) + "')"

    db = get_db()
    query = 'SELECT SUM(bytes) AS bytes \
            FROM connections, probes \
            WHERE \
            connections.ID == probes.connection \
            AND \
            (\
                (dst_addr = \'' + str(addr) + '\' \
                OR \
                (dst_addr like \'127%\' AND client = \'' + str(addr) + '\')\
            ) \
            AND dst_port in ' + str(ports) + ')'
        
        # and ts > ' + str(time) + '\
        
    cur = db.execute(query)
    data = cur.fetchone()

    return data[0] if data else -1

def getWorkerDataOut(addr, ports):# , time):

    if len(ports) == 1: ports = "('" + str(ports[0]) + "')"

    db = get_db()
    query = 'SELECT SUM(bytes) as bytes \
            FROM connections, probes \
            WHERE \
            connections.ID == probes.connection \
            AND \
            (\
                (src_addr = \'' + str(addr) + '\' \
                OR \
                (src_addr like \'127%\' AND client = \'' + str(addr) + '\')\
            ) \
            AND \
            src_port in ' + str(ports) + ')'
    
    # print(query)
        # and ts > ' + str(time) + '\

    cur = db.execute(query)
    data = cur.fetchone()

    return data[0] if data else -1

def getAggregate(cur):
    totPkts = 0
    totData = 0
    
    for row in cur:
        totPkts += int(row[5])
        totData += int(row[6])

    return (totPkts, totData)



def updateStormDb():
    lock.acquire()
    db = connect_db()

    #### update supervisor table
    query = 'SELECT host, uptime FROM supervisor'
    cur = db.execute(query)
    hostsInDb = {r[0]:r[1] for r in cur.fetchall()}
    for s in storm.supervisors:
        if s[0] in [k for k in hostsInDb]:
            if hostsInDb[s[0]] != str(s[1]):
                ### update
                query = 'UPDATE supervisor SET uptime = ' + str(s[1]) + ' WHERE host = \'' + s[0] + '\''
                db.execute(query)
                # db.commit()
        else:
            ### insert
            query = 'INSERT INTO supervisor (host, uptime) VALUES (\'' + s[0] + '\',' + str(s[1]) + ')'
            db.execute(query)
            # db.commit()
            
    #### update topology table
    query = 'SELECT ID, name FROM topology'
    cur = db.execute(query)
    topologiesInDb = {r[0]:r[1] for r in cur.fetchall()}
    for t in storm.topologies:
        if t not in [k for k in topologiesInDb]:
            ### insert
            query = 'INSERT INTO topology (ID, name) VALUES (\'' + t + '\',\'' + storm.topologies[t] + '\')'
            db.execute(query)
            # db.commit()

    #### update workers table
    query = 'SELECT host, port, topoID FROM worker'
    cur = db.execute(query)
    workersInDb = [(r[0],r[1],r[2]) for r in cur.fetchall()]
    for topo in storm.workers: ## worker[topoId] = [(host,port),(host,port),...]
        if sorted(storm.workers[topo]) != sorted([(e[0],int(e[1])) for e in workersInDb if e[2] == topo]):
            ### if the two lists are not equal it means: (A) there is a new worker, (B) thre isn't anymore an old worker
            ### is it always true? storm can't add new workers on runtime, a crashed worker will come up again with same host and port
            ### if the two lists are not equal it means: there arent' the workers of this topology in the database, so:
            ### insert
            for couple in storm.workers[topo]:
                query = 'INSERT INTO worker (host, port, topoID) VALUES (\'' + couple[0] + '\',\'' + str(couple[1]) + '\',\'' + topo + '\')'
                db.execute(query)
                # db.commit()
            
    #### update component table
    query = 'SELECT ID, topoID FROM component'
    cur = db.execute(query)
    componentsInDb = [(r[0],r[1]) for r in cur.fetchall()]
    for topo in storm.components: ## component[topoId] = [spout,spout,bolt,bolt,...]
        if sorted(storm.components[topo]) != sorted([e[0] for e in componentsInDb if e[1] == topo]):
            ### same as above
            ### insert
            for c in storm.components[topo]:
                query = 'INSERT INTO component (ID, topoID) VALUES (\'' + c + '\',\'' + topo + '\')'
                db.execute(query)
                # db.commit()
    
    #### update executor table 
    query = 'SELECT executor, host, port, component FROM executor'
    cur = db.execute(query)
    executorsInDb = {r[3]:(r[0],r[1],r[2]) for r in cur.fetchall()}
    for topo in storm.executors: ## executor[topoId][component] = [(id,host,port),(id,host,port),...]
        for component in storm.executors[topo]:
            for e in storm.executors[topo][component]:
                if component not in executorsInDb:
                    ## insert
                    query = 'INSERT INTO executor (executor, host, port, component) ' + \
                            'VALUES (\'' + e[0] + '\',\'' + e[1] + '\',\'' + str(e[2]) + '\',\'' + component + '\')'
                elif e[1] and (executorsInDb[component][1] != e[1] or executorsInDb[component][2] != e[2]):
                    ## update if something changed
                    query = 'UPDATE executor ' + \
                            'SET host = \'' + e[1] + '\' AND port = \'' + str(e[2]) + '\' ' + \
                            'WHERE executor = \'' + e[0] + '\' AND component = \'' + component + '\'' 
                db.execute(query)
    
    db.commit()
    db.close()
    lock.release()

def checkStormDb():
    print('Retreiving data from local DB... ', end='')
    ### follow storm.reload() scheme
    db = connect_db()
    ### get supervisors data
    query = 'SELECT host, uptime FROM supervisor'
    cur = db.execute(query)
    storm.supervisors = [(r[0],r[1]) for r in cur.fetchall()]
    ### get topologies
    if len(storm.supervisors) > 0:
        query = 'SELECT ID, name FROM topology'
        cur = db.execute(query)
        storm.topologies = {r[0]:r[1] for r in cur.fetchall()}
        if len(storm.topologies) > 0:
            for topo in storm.topologies:
            ### if there are topologies get workers
                query = 'SELECT host, port FROM worker WHERE topoID = \'' + topo + '\''
                cur = db.execute(query)
                storm.workers[topo] = [(r[0],r[1]) for r in cur.fetchall()]
            ### get components of the topology
                query = 'SELECT ID FROM component WHERE topoID = \'' + topo + '\''
                cur = db.execute(query)
                storm.components[topo] = [r[0] for r in cur.fetchall()]
            ### get executors for each component 
                ## problem: same name components from different topologies, filter by host and port
                storm.executors[topo] = {}
                for compo in storm.components[topo]:
                    hosts = tuple(w[0] for w in storm.workers[topo])
                    ports = tuple(w[1] for w in storm.workers[topo])
                    query = 'SELECT executor, host, port FROM executor WHERE component = \'' + compo + '\'' + \
                            'AND host IN ' + str(hosts) + ' AND port IN ' + str(ports) 
                    cur = db.execute(query)
                    storm.executors[topo][compo] = [(r[0],r[1],r[2]) for r in cur.fetchall()]
        else:
            print('FAILED (no topo in db)') 
            return False ## no topologies in the database
    else:
        print('FAILED (no supervisors in db)')  
        return False ## update failed at supervisors
    
    print('SUCCESS')
    return True

def reload_storm():
    global storm, gotFromDb
    now = time.time()
    res = True
    if now - storm.lastUpdate > 600: 
        # print(storm.lastUpdate - now)
        res = storm.reload()
        if res: 
            updateStormDb()
            gotFromDb = False
            
    if len(storm.topologies) < 1 or not res:
        ### check in the database
        gotFromDb = checkStormDb()

# @app.teardown_appcontext
# def close_db(error):
#     """Closes the database again at the end of the request."""
#     if hasattr(g, 'sqlite_db'):
#         g.sqlite_db.close()



# @app.before_first_request
# def init():
#     init_db() if args.initdb else None
    
# @app.route("/")
# @app.route("/index")
# @app.route("/conn_view")
# def conn_view():
#     connections = getSummary()
#     cons = []
#     now = time.time()
#     for c in connections:
#         ts = float(c[6])
#         if  now - ts < 60*60:
#             connString = c[0]+':'+c[1]+' -> '+c[2]+':'+c[3]
#             cons.append((connString, humansize(int(c[4]), False), humansize(int(c[5])), datetime.datetime.fromtimestamp(ts).strftime("%H:%M:%S")))
#     return render_template('summary.html',connections=cons)

# @app.route('/topo_view')
# @app.route('/topo_view/')

def topo_view():
    global storm

    reload_storm()

    topoSummary = []

    if storm.topologies:
        for topo in storm.topologies:
            # print(storm.lastUpdate, gotFromDb, storm.workers[topo], len(storm.workers[topo]))
            if (storm.lastUpdate > 0 or gotFromDb) and storm.workers[topo] and len(storm.workers[topo]) > 0:
                net = getAggregate(getTopoNetwork(storm.getWorkersAddr(topo), storm.getWorkersPort(topo)))#, time.time() - storm.getLastUp(topo)))
                topoSummary.append((topo, storm.topologies[topo], len(storm.workers[topo]), humansize(net[0], False), humansize(net[1])))
            else: topoSummary.append((topo, storm.topologies[topo], 'scheduling...', 'ND', 'ND'))

    return render_template('topology.html', topo_summary=topoSummary)

def getTopologyConnections(topoId, portMap):
    connections = []

    net = getTopoNetwork(storm.getWorkersAddr(topoId), storm.getWorkersPort(topoId))#, time.time() - storm.getLastUp(topoId))
    for row in net:
        
        sourceWorker = storm.getNameByIp(row[1])
        if not sourceWorker: sourceWorker = storm.getNameByIp(row[0])
        destinationWorker = storm.getNameByIp(row[3])
        if not destinationWorker: destinationWorker = storm.getNameByIp(row[0])

        sourceWorkerIp = storm.getIpByName(sourceWorker)
        destinationWorkerIp = storm.getIpByName(destinationWorker)

        sourcePort = row[2]
        destPort = row[4]

        connString = sourceWorker + ':' + sourcePort + ' -> ' + destinationWorker + ':' + destPort
        
        if (sourceWorkerIp, sourcePort) in portMap: sourcePid = portMap[(sourceWorkerIp, sourcePort)]
        else: sourcePid = 'NA'
        if (destinationWorkerIp, destPort) in portMap: destPid = portMap[(destinationWorkerIp, destPort)]
        else: destPid = 'NA'

        workerString = sourcePid + ' -> ' + destPid

        connections.append((connString, workerString, humansize(int(row[5]), False), humansize(int(row[6])), datetime.datetime.fromtimestamp(float(row[7])).strftime("%H:%M:%S")))
    
    return connections

def getWorkersView(topoId, portMap):
    executors = {}
    
    reload_storm()

    for worker in storm.workers[topoId]:
        executors[(worker[0], worker[1])] = []

    for component in storm.components[topoId]:
        for executor in storm.executors[topoId][component]:
            executors[(executor[1], executor[2])].append(str(component) + str(executor[0]))

    ret = [] 
    for element in executors:
        ip = storm.getIpByName(element[0])
        ports = [element[1]]
        if (ip, str(element[1])) in portMap:
            pid = portMap[(ip, str(element[1]))]
            ports = [k[1] for k,v in portMap.items() if v==str(pid)]

        # print(pid, ports)
        in_data = getWorkerDataIn(ip, tuple(ports))#, time.time() - storm.getLastUp(topoId))
        out_data = getWorkerDataOut(ip, tuple(ports))#, time.time() - storm.getLastUp(topoId))

        if(not in_data): in_data = -1
        if (not out_data): out_data = -1

        if (ip, str(element[1])) in portMap:
            pid = portMap[(ip, str(element[1]))] 
        else: pid = 'NA'
        ret.append((pid, str(element[0]) + ':' + str(element[1]), len(executors[element]), ', '.join(executors[element]), humansize(int(in_data)), humansize(int(out_data))))

    return ret

def getPortMap():
    
    db = get_db()
    query = 'select addr, port, pid from port_mapping'
    cur = db.execute(query)

    portmap = {}    
    for row in cur.fetchall():
        portmap[(row[0], row[1])] = row[2]

    return portmap

# @app.route('/topo_view/network', methods=['GET'])
# def topo_network():
#     global storm 
#     connections = workers = "Error"
#     topo_name = "No ID Selected"

#     if request.args['id']:
#         topoId = request.args['id']
    
#     try:    
#         reload_storm()

#         portMap = getPortMap()
#         workers = getWorkersView(topoId, portMap)
#         connections = getTopologyConnections(topoId, portMap)

#         topo_name = storm.topologies[topoId] + " [" + topoId + "]" 

#         return render_template('topo_network.html', connections=connections, workers=workers, topo_name=topo_name)

#     except:
#         e = sys.exc_info()[0]
#         print("DEBUG: {}".format(e))
#         raise
#         # return redirect(url_for('topo_view'))

def getConnection(sa,sp,da,dp):
    db = get_db()
    query = 'select ID, client from connections where src_addr == \'' + sa + '\' and src_port == \'' + sp + '\' and dst_addr == \'' + da + '\' and dst_port == \'' + dp + '\''
    cur = db.execute(query)  
    r = list(cur.fetchall())
    if len(r) < 1: return None
    else: return r


# def dbNetworkInsert(client, ts, src_host,src_port,dst_host,dst_port, bts, pkts):
#     c = getConnection(src_host, src_port, dst_host, dst_port)
#     if c and client != c[0][1]: pass
#     else:
#         if not c:
#             query = 'insert into connections (client, src_addr, src_port, dst_addr, dst_port) values (\'' + str(client) + '\',\'' + src_host + '\',\'' + src_port + '\',\'' + dst_host + '\',\'' + dst_port + '\')'
#             connId = insert_db(query)
#         else: connId = c[0][0]
#         query = 'insert into probes (connection, ts, pkts, bytes) values (' + str(connId) + ',\'' + str(ts) + '\',\'' + pkts + '\',\'' + bts + '\')'
#         insert_db(query)

#     print ("[VERBOSE] Data write success") if args.verbose else None


# @app.route("/api/v0.2/network/insert", methods=['POST'])
# def networkInsert_api_v2():
#     try:
#         reload_storm()
#         client = request.remote_addr
#         ts = request.form["ts"]
#         print("[DEBUG] received length trace:",len(request.form)) if args.verbose else None
#         for key in request.form: 
#             # ts = time.time()
#             if key != "ts":
#                 k = key[1:-1].split(',')
#                 src_host = ''.join(k[0].split('\'')).strip()
#                 src_port = ''.join(k[1].split('\'')).strip()
#                 dst_host = ''.join(k[2].split('\'')).strip()
#                 dst_port = ''.join(k[3].split('\'')).strip()
#                 # print(src_host,src_port,dst_host,dst_port)
#                 v = request.form[key].split(',')
#                 bts = v[0]
#                 pkts = v[1]
#                 # print(request.form[key],v,pkts,bts)

#                 # print(client,ts,src_host,src_port,dst_host,dst_port,pkts,bts)
#                 netInsert(client, ts, src_host,src_port,dst_host,dst_port, bts, pkts)
                
#     except:
#         print ("[ERROR] : networkInsert_v2 : Wrong API request")
#         abort(400)
        
#     return "Ok"

def networkInsert(payload):
    try:
        reload_storm()
        client = payload["client"]
        ts = payload["ts"]
        print("[DEBUG] received length trace:",len(request.form)) if args.verbose else None

        probesQueries = []

        for key in payload: 
            # ts = time.time()
            if key != "ts" and key != "client":
                k = key[1:-1].split(',')
                src_host = ''.join(k[0].split('\'')).strip()
                src_port = ''.join(k[1].split('\'')).strip()
                dst_host = ''.join(k[2].split('\'')).strip()
                dst_port = ''.join(k[3].split('\'')).strip()
                # print(src_host,src_port,dst_host,dst_port)
                v = payload[key].split(',')
                bts = v[0]
                pkts = v[1]
                # print(request.form[key],v,pkts,bts)

                # print(client,ts,src_host,src_port,dst_host,dst_port,pkts,bts)
                c = getConnection(src_host, src_port, dst_host, dst_port)
                if c and client != c[0][1]: pass
                else:
                    if not c:
                        query = 'insert into connections (client, src_addr, src_port, dst_addr, dst_port) values (\'' + str(client) + '\',\'' + src_host + '\',\'' + src_port + '\',\'' + dst_host + '\',\'' + dst_port + '\')'
                        connId = insert_db(query)
                        
                    else: connId = c[0][0]
                    query = 'insert into probes (connection, ts, pkts, bytes) values (' + str(connId) + ',\'' + str(ts) + '\',\'' + pkts + '\',\'' + bts + '\')'
                    probesQueries.append(query)


                multi_insert_db(probesQueries)
                
                # dbNetworkInsert(client, ts, src_host,src_port,dst_host,dst_port, bts, pkts)
                
    except:
        print ("[ERROR] : Wrong payload received in network insert")


def getPort(addr, port):
    db = get_db()
    query = 'select pid from port_mapping where addr == \'' + addr + '\' and port == \'' + port + '\''
    cur = db.execute(query)    
    r = list(cur.fetchall())
    if len(r) < 1: return None
    else: return r

def dbPortInsert(client, port, pid):
    p = getPort(client, port)
    if not p:
        query = 'insert into port_mapping (addr, port, pid) values (\'' + client + '\',\'' + port + '\',\'' + pid + '\')'
        # connId = insert_db(query)
        return query
    elif p[0][0] != pid: #update the database with the new pid (should happen only if the worker crashes)
        query = 'update port_mapping set pid = \'' + pid + '\' where addr == \'' + client + '\' and port == \'' + port + '\''
        # insert_db(query)
        return query

# @app.route("/api/v0.2/port/insert", methods=['POST'])
# def portInsert_api_v2():
#     try:
#         reload_storm()
#         client = request.remote_addr
#         # ts = time.time()
#         for key in request.form:
#             port = key
#             pid = request.form[key]
#             # print(client,port,pid)
            
#             portInsert(client, port, pid)
#     except:
#         print ("[ERROR] Wrong API request")
#         abort(400)
#     return "Ok"

def portInsert(payload):
    try:
        reload_storm()
        # print("here")
        client = payload["client"]
        # print("client:   ", client)
        # ts = time.time()

        portQueries = []

        for key in payload:
            # print("new key:    ", key)
            if key != "client":
                port = key
                pid = payload[key]
                # print(client,port,pid)
            
                portQueries.append(dbPortInsert(client, port, pid))
        
        multi_insert_db(portQueries)

    except Exception as e:
        print ("[ERROR] Wrong payload in port insert")
        print(e)


def networkKafkaConsumer(kafkaServerAddress):
    try:
        consumer = KafkaConsumer(networkTopicName, bootstrap_servers=kafkaServerAddress)
    except Exception as e:
        print("Error: ",e)

    for msg in consumer:
        m = msg.value
        s = m.decode(encodingMethod)
        d = json.loads(s)
        # print("network received:   ",d)
        networkInsert(d)


def portKafkaConsumer(kafkaServerAddress):
    consumer = KafkaConsumer(portTopicName, bootstrap_servers=kafkaServerAddress)
    for msg in consumer:
        m = msg.value
        s = m.decode(encodingMethod)
        d = json.loads(s)
        # print("port received:   ",d)
        portInsert(d)

def is_running(t):
    # if is_locked() or last_mod() - time.time() < t: return True # ERROR
    # else: return False
    isl = is_locked()
    lm = last_mod()
    tim = time.time()
    lmd = tim - lm
    lmb = lmd < t
    return (isl, tim, lm, lmd, t, lmb, isl or lmb) ## GOOD ONE

def last_mod():
    return getmtime(db_dir)

def is_locked():
    global lock
    return lock.locked()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter, description="Start netmonitor server")
    parser.add_argument("-p", "--port", dest="port", type=int, help="specify listening port", default=5000)
    parser.add_argument("-k", "--kafka", dest="kafka_address", help="specify kafka server address")
    parser.add_argument("-v", "--verbose", dest="verbose", help="verbose mode", action="store_true", default=False)
    parser.add_argument("--initdb", dest="initdb", help="initialize the database", action="store_true", default=False)
    # parser.add_argument("-w", dest="csv", help="specify a csv file to store results", type=str) # TODO: write to csv file if selected
    parser.add_argument("--nimbus", dest="nimbus_address", help="storm nimbus address", type=str, default=None)    
    args = parser.parse_args()

    init_db() if args.initdb else None

    if args.kafka_address: kafkaServerAddress = args.kafka_address
    else: kafkaServerAddress = socket.gethostname()

    if(args.nimbus_address):
        storm = StormCollector(args.nimbus_address)
    else:
        print("You must define nimbus address [--nimbus]")
        quit()
        
    print(" * Enabled verbose output * ") if args.verbose else None

    thread.start_new_thread(networkKafkaConsumer, (kafkaServerAddress,))
    thread.start_new_thread(portKafkaConsumer, (kafkaServerAddress,))

    print("Ctrl+C to kill the application")
    
    from xmlrpc.server import SimpleXMLRPCServer

    server = SimpleXMLRPCServer(("0.0.0.0", 8585))
    server.register_function(is_running, "is_running")
    server.register_function(is_locked, "is_locked")
    server.register_function(last_mod, "last_mod")
    server.serve_forever()

    # app.run(host='0.0.0.0', port=args.port, threaded=True)