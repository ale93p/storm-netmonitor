from flask import Flask, jsonify, request, abort, render_template, g
from modules.stormapi import StormCollector
import argparse
import csv
import os.path
import socket
import time, datetime
import sqlite3


start_time = time.time()
filename = 'network_db_' + time.strftime("%d%m%y%H%M%s") + '.csv'
db_dir = 'database/netmonitor.db'
schema_dir = 'database/schema.sql'
nimbus_address = 'sdn1.i3s.unice.fr'
localhost = ['127.0.0.1','127.0.1.1'] 


storm = StormCollector(nimbus_address)

app = Flask(__name__)
app.config.from_object(__name__)
app.config.update(dict(
    DATABASE=os.path.join(app.root_path, db_dir),
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

def writeToCsv(d): 
    title_row = ['client', 'timestamp', 'snd_addr', 'snd_port', 'rcv_addr', 'rcv_port', 'pkts', 'bytes']
    if len(d) != len(title_row): raise Exception('Data lenght not matching')
    newfile = not os.path.isfile(filename)
    with open(filename, 'a', newline='') as csvfile:
        csvwriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        if newfile: csvwriter.writerow(title_row)
        csvwriter.writerow(d)

def getConnection(sa,sp,da,dp):
    db = get_db()
    query = 'select ID, client from connections where src_addr == \'' + sa + '\' and src_port == \'' + sp + '\' and dst_addr == \'' + da + '\' and dst_port == \'' + dp + '\''
    cur = db.execute(query)    
    r = list(cur.fetchall())
    if len(r) < 1: return None
    else: return r

def getSummary():
    db = get_db()
    query = 'select src_addr, src_port, dst_addr, dst_port, SUM(pkts) as pkts, SUM(bytes) as bytes, MAX(ts) as ts from connections, probes where connections.ID == probes.connection group by probes.connection order by bytes desc;'
    cur = db.execute(query)
    return cur.fetchall()

def getTopoNetwork(addrs, ports):
    db = get_db()
    query = 'select client, src_addr, src_port, dst_addr, dst_port, SUM(pkts) as pkts, SUM(bytes) as bytes \
        from connections, probes \
        where connections.ID == probes.connection and \
        ((src_addr in ' + str(addrs) + ' and src_port in ' + str(ports) + ') or (dst_addr in ' + str(addrs) + ' and dst_port in ' + str(ports) + ')) \
        and ts > ' + str(start_time - 60*60*3) + '\
        group by probes.connection \
        order by bytes desc'
    cur = db.execute(query)
    return cur.fetchall()

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
def topo_view():
    storm.reload()
    topoSummary = []
    for topo in storm.topologies:
        net = getAggregate(getTopoNetwork(storm.getWorkersAddr(topo), storm.getWorkersPort(topo)))
        topoSummary.append((topo, storm.topologies[topo], len(storm.workers[topo]), humansize(net[0], False), humansize(net[1])))
    return render_template('topology.html', topo_summary=topoSummary, topo_name=storm.topologies[topo])

def getTopologyConnections(topoId):
    connections = []

    net = getTopoNetwork(storm.getWorkersAddr(topoId), storm.getWorkersPort(topoId))
    for row in net:
        
        sourceWorker = storm.getNameByIp(row[1])
        if not sourceWorker: sourceWorker = storm.getNameByIp(row[0])
        destinationWorker = storm.getNameByIp(row[3])
        if not destinationWorker: destinationWorker = storm.getNameByIp(row[0])

        connString = sourceWorker.split('.',1)[0] + ':' + row[2] + ' -> ' + destinationWorker.split('.',1)[0] + ':' + row[4]

        connections.append((connString, humansize(int(row[5]), False), humansize(int(row[6]))))
    return connections

def getWorkersView(topoId):
    mapping = {}
    
    for worker in storm.workers[topoId]:
        mapping[(worker[0], worker[1])] = []

    for component in storm.components[topoId]:
        for executor in storm.executors[topoId][component]:
            mapping[(executor[1], executor[2])].append(component)
    
    workers = [] 
    for element in mapping:
        workers.append((str(element[0]) + ':' + str(element[1]), len(mapping[element]), ', '.join(mapping[element])))

    return workers

@app.route('/topo_view/network', methods=['GET'])
def topo_network():
    
    if request.args['id']:
        topoId = request.args['id']
        storm.reload()

        connections = getTopologyConnections(topoId)
        workers = getWorkersView(topoId)
        
    return render_template('topo_network.html', connections=connections, workers=workers, topo_name=storm.topologies[topoId])

@app.route("/api/v0.1/network/insert", methods=['GET'])
def networkInsert():
    try:
        client = request.remote_addr
        ts = time.time()
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
    
    c = getConnection(src_host, src_port, dst_host, dst_port)
    if c and client != c[0][1]: pass
    else:
        if not c:
            query = 'insert into connections (client, src_addr, src_port, dst_addr, dst_port) values (\'' + client + '\',\'' + src_host + '\',\'' + src_port + '\',\'' + dst_host + '\',\'' + dst_port + '\')'
            connId = insert_db(query)
        else: connId = c[0][0]
        query = 'insert into probes (connection, ts, pkts, bytes) values (' + str(connId) + ',\'' + str(ts) + '\',\'' + pkts + '\',\'' + bts + '\')'
        insert_db(query)

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

    app.run(host='0.0.0.0', port=args.port)
