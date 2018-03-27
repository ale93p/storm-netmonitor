from flask import Flask, jsonify, request, abort, render_template, g, redirect, url_for 
import argparse
import time, datetime
import sqlite3
#import csv
from os.path import join #, isfile
from modules.stormapi import StormCollector

start_time = time.time()
#filename = 'network_db_' + time.strftime("%d%m%y%H%M%s") + '.csv'
db_dir = 'database/netmonitor.db'
schema_dir = 'database/schema.sql'
nimbus_address = 'sdn1.i3s.unice.fr'
localhost = ['127.0.0.1','127.0.1.1'] 

storm = StormCollector(nimbus_address)
gotFromDb = False

app = Flask(__name__)
app.config.from_object(__name__)
app.config.update(dict(
    DATABASE=join(app.root_path, db_dir),
    SECRET_KEY='development key',
    USERNAME='admin',
    PASSWORD='default'
))
app.config.from_envvar('FLASKR_SETTINGS', silent=True)

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

#def writeToCsv(d): 
#    title_row = ['client', 'timestamp', 'snd_addr', 'snd_port', 'rcv_addr', 'rcv_port', 'pkts', 'bytes']
#    if len(d) != len(title_row): raise Exception('Data lenght not matching')
#    newfile = not isfile(filename)
#    with open(filename, 'a', newline='') as csvfile:
#        csvwriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
#        if newfile: csvwriter.writerow(title_row)
#        csvwriter.writerow(d)

def getSummary():
    db = get_db()
    query = 'select src_addr, src_port, dst_addr, dst_port, SUM(pkts) as pkts, SUM(bytes) as bytes, MAX(ts) as ts from connections, probes where connections.ID == probes.connection group by probes.connection order by bytes desc;'
    cur = db.execute(query)
    return cur.fetchall()

def getTopoNetwork(addrs, ports):#, time):
    db = get_db()
    query = 'select client, src_addr, src_port, dst_addr, dst_port, SUM(pkts) as pkts, SUM(bytes) as bytes, MAX(ts) \
        from connections, probes \
        where connections.ID == probes.connection and \
        ((src_addr in ' + str(addrs) + ' and src_port in ' + str(ports) + ') or (dst_addr in ' + str(addrs) + ' and dst_port in ' + str(ports) + ')) \
        group by probes.connection \
        order by bytes desc'

        #and ts > ' + str(time) + '\
    cur = db.execute(query)
    return cur.fetchall()

def getWorkerDataIn(addr, ports):#, time):

    if len(ports) == 1: ports = "('" + str(ports[0]) + "')"

    db = get_db()
    query = 'select SUM(bytes) as bytes \
        from connections, probes \
        where connections.ID == probes.connection \
        and (dst_addr = \'' + str(addr) + '\' and dst_port in ' + str(ports) + ') \
        group by dst_addr'
        
        # and ts > ' + str(time) + '\
        
    cur = db.execute(query)
    data = cur.fetchone()

    return data[0] if data else -1

def getWorkerDataOut(addr, ports):# , time):

    if len(ports) == 1: ports = "('" + str(ports[0]) + "')"

    db = get_db()
    query = 'select SUM(bytes) as bytes \
        from connections, probes \
        where connections.ID == probes.connection \
        and (src_addr = \'' + str(addr) + '\' and src_port in ' + str(ports) + ') \
        group by src_addr'
    
    print(query)
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

def insert_db(query):
    db = get_db()
    cur = db.execute(query)
    db.commit()
    return cur.lastrowid

def connect_db():
    """Connects to the specific database."""
    rv = sqlite3.connect(app.config['DATABASE'])
    rv.row_factory = sqlite3.Row
    return rv

def get_db():
    """Opens a new database connection if there is none yet for the
    current application context.
    """
    if not hasattr(g, 'sqlite_db'):
        g.sqlite_db = connect_db()
    return g.sqlite_db

def updateStormDb():
    db = get_db()

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
                db.commit()
        else:
            ### insert
            query = 'INSERT INTO supervisor (host, uptime) VALUES (\'' + s[0] + '\',' + str(s[1]) + ')'
            db.execute(query)
            db.commit()
            
    #### update topology table
    query = 'SELECT ID, name FROM topology'
    cur = db.execute(query)
    topologiesInDb = {r[0]:r[1] for r in cur.fetchall()}
    for t in storm.topologies:
        if t not in [k for k in topologiesInDb]:
            ### insert
            query = 'INSERT INTO topology (ID, name) VALUES (\'' + t + '\',\'' + storm.topologies[t] + '\')'
            db.execute(query)
            db.commit()

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
                db.commit()
            
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
                db.commit()
    
    #### update executor table 
    query = 'SELECT executor, host, port, component FROM executor'
    cur = db.execute(query)
    executorsInDb = {r[3]:(r[0],r[1],r[2]) for r in cur.fetchall()}
    for topo in storm.executors: ## executor[topoId][component] = [(id,host,port),(id,host,port),...]
        for component in storm.executors[topo]:
            for e in storm.executors[topo][component]:
                if component not in executorsInDb:
                    query = 'INSERT INTO executor (executor, host, port, component) ' + \
                            'VALUES (\'' + e[0] + '\',\'' + e[1] + '\',\'' + str(e[2]) + '\',\'' + component + '\')'
                elif executorsInDb[component][1] != e[1] and executorsInDb[component][2] != e[2]:
                    query = 'UPDATE executor ' + \
                            'SET host = \'' + e[1] + '\' AND port = \'' + str(e[2]) + '\' ' + \
                            'WHERE executor = \'' + e[0] + '\' AND component = \'' + component + '\'' 
                db.execute(query)
                db.commit()

def checkStormDb():
    global gotFromDb
    print('Retreiving data from local DB... ', end='')
    ### follow storm.reload() scheme
    db = get_db()
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
    global storm, storm_offline, gotFromDb
    now = time.time()
    res = True
    if now - storm.lastUpdate > 600: 
        print(storm.lastUpdate - now)
        res = storm.reload()
        if res: 
            updateStormDb()
            gotFromDb = False
            
    if len(storm.topologies) < 1 or not res:
        ### use storm_offline, if not present: 
        ### check in the database
        gotFromDb = checkStormDb()

@app.teardown_appcontext
def close_db(error):
    """Closes the database again at the end of the request."""
    if hasattr(g, 'sqlite_db'):
        g.sqlite_db.close()

def init_db():
    db = get_db()
    with app.open_resource(schema_dir, mode='r') as f:
        db.cursor().executescript(f.read())
    db.commit()

@app.before_first_request
def init():
    init_db() if args.initdb else None
    
@app.route("/")
@app.route("/index")
@app.route("/conn_view")
def conn_view():
    connections = getSummary()
    cons = []
    now = time.time()
    for c in connections:
        ts = float(c[6])
        if  now - ts < 60*60:
            connString = c[0]+':'+c[1]+' -> '+c[2]+':'+c[3]
            cons.append((connString, humansize(int(c[4]), False), humansize(int(c[5])), datetime.datetime.fromtimestamp(ts).strftime("%H:%M:%S")))
    return render_template('summary.html',connections=cons)

@app.route('/topo_view')
@app.route('/topo_view/')
def topo_view():
    global storm, storm_offline

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

        in_data = getWorkerDataIn(ip, tuple(ports))#, time.time() - storm.getLastUp(topoId))
        out_data = getWorkerDataOut(ip, tuple(ports))#, time.time() - storm.getLastUp(topoId))

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

@app.route('/topo_view/network', methods=['GET'])
def topo_network():
    global storm, storm_offline
    connections = workers = "Error"
    topo_name = "No ID Selected"

    if request.args['id']:
        topoId = request.args['id']
    
    try:    
        reload_storm()

        portMap = getPortMap()
        workers = getWorkersView(topoId, portMap)
        connections = getTopologyConnections(topoId, portMap)

        topo_name = storm.topologies[topoId] + " [" + topoId + "]" 

        return render_template('topo_network.html', connections=connections, workers=workers, topo_name=topo_name)

    except:
        return redirect(url_for('topo_view'))

def getConnection(sa,sp,da,dp):
    db = get_db()
    query = 'select ID, client from connections where src_addr == \'' + sa + '\' and src_port == \'' + sp + '\' and dst_addr == \'' + da + '\' and dst_port == \'' + dp + '\''
    cur = db.execute(query)  
    r = list(cur.fetchall())
    if len(r) < 1: return None
    else: return r


def netInsert(client, ts, src_host,src_port,dst_host,dst_port, bts, pkts):
    c = getConnection(src_host, src_port, dst_host, dst_port)
    if c and client != c[0][1]: pass
    else:
        if not c:
            query = 'insert into connections (client, src_addr, src_port, dst_addr, dst_port) values (\'' + str(client) + '\',\'' + src_host + '\',\'' + src_port + '\',\'' + dst_host + '\',\'' + dst_port + '\')'
            connId = insert_db(query)
        else: connId = c[0][0]
        query = 'insert into probes (connection, ts, pkts, bytes) values (' + str(connId) + ',\'' + str(ts) + '\',\'' + pkts + '\',\'' + bts + '\')'
        insert_db(query)

    print ("[VERBOSE] Data write success") if args.verbose else None


@app.route("/api/v0.2/network/insert", methods=['POST'])
def networkInsert_v2():
    try:
        client = request.remote_addr
        ts = request.form["ts"]
        print("length trace:",len(request.form))
        for key in request.form: 
            # ts = time.time()
            if key != "ts":
                k = key[1:-1].split(',')
                src_host = ''.join(k[0].split('\'')).strip()
                src_port = ''.join(k[1].split('\'')).strip()
                dst_host = ''.join(k[2].split('\'')).strip()
                dst_port = ''.join(k[3].split('\'')).strip()
                # print(src_host,src_port,dst_host,dst_port)
                v = request.form[key].split(',')
                pkts = v[0]
                bts = v[1]

                # print(client,ts,src_host,src_port,dst_host,dst_port,pkts,bts)
                netInsert(client, ts, src_host,src_port,dst_host,dst_port, bts, pkts)
                
    except:
        print ("[ERROR] : networkInsert_v2 : Wrong API request")
        abort(400)
        
    return "Ok"


@app.route("/api/v0.1/network/insert", methods=['GET'])
def networkInsert_v1():
    """ deprecated """
    try:
        client = request.remote_addr
        # ts = time.time()
        ts = request.args["ts"]
        src_host = request.args["src_host"]
        src_port = request.args["src_port"]
        dst_host = request.args["dst_host"]
        dst_port = request.args["dst_port"]
        pkts = request.args["pkts"]
        bts = request.args["bytes"]
    except:
        print ("[ERROR] Wrong API request")
        abort(400)
    print ("[VERBOSE] Received from ",client," at ",str(ts),": ",src_host,":",src_port,"->",dst_host,":",dst_port," ",pkts,"pkts ",bts,"Bytes)", sep='') if args.verbose else None
    
    netInsert(client, ts, src_host,src_port,dst_host,dst_port, bts, pkts)

    return "Ok"

def getPort(addr, port):
    db = get_db()
    query = 'select pid from port_mapping where addr == \'' + addr + '\' and port == \'' + port + '\''
    cur = db.execute(query)    
    r = list(cur.fetchall())
    if len(r) < 1: return None
    else: return r

def portInsert(client, port, pid):
    p = getPort(client, port)
    if not p:
        query = 'insert into port_mapping (addr, port, pid) values (\'' + client + '\',\'' + port + '\',\'' + pid + '\')'
        connId = insert_db(query)
    elif p[0][0] != pid: #update the database with the new pid (should happen only if the worker crashes)
        query = 'update port_mapping set pid = \'' + pid + '\' where addr == \'' + client + '\' and port == \'' + port + '\''
        insert_db(query)

@app.route("/api/v0.2/port/insert", methods=['POST'])
def portInsert_v2():
    try:
        client = request.remote_addr
        # ts = time.time()
        for key in request.form:
            port = key
            pid = request.form[key]
            portInsert(client, port, pid)
    except:
        print ("[ERROR] Wrong API request")
        abort(400)
    return "Ok"

@app.route("/api/v0.1/port/insert", methods=['GET'])
def portInsert_v1():
    """ deprecated """
    try:
        client = request.remote_addr
        ts = time.time()
        port = request.args["port"]
        pid = request.args["pid"]
    except:
        print ("[ERROR] Wrong API request")
        abort(400)
    print ("[VERBOSE] Received from ",client," at ",str(ts),": ", port," <-> ", pid, sep='') if args.verbose else None
    
    portInsert(client, port, pid)

    print ("[VERBOSE] Data write success") if args.verbose else None
    return "Ok"

if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter, description="Start netmonitor server")
    parser.add_argument("-p", "--port", dest="port", type=int, help="specify listening port", default=5000)
    parser.add_argument("-v", "--verbose", dest="verbose", help="verbose mode", action="store_true", default=False)
    parser.add_argument("--initdb", dest="initdb", help="initialize the database", action="store_true", default=False)
    # parser.add_argument("-w", dest="csv", help="specify a csv file to store results", type=str) # TODO: write to csv file if selected
    args = parser.parse_args()

    print(" * Enabled verbose output * ") if args.verbose else None

    app.run(host='0.0.0.0', port=args.port, threaded=True)
