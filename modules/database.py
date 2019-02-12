import sqlite3
import traceback
import time


### Basic DB Operations

def db_connect():
    """Connects to the memory database."""
    database_uri = 'file:netmonitor_database?mode=memory&cache=shared'
    rv = sqlite3.connect(database_uri, uri = True)
    rv.row_factory = sqlite3.Row
    return rv

def init_db(db, schema_dir):
    print("Initilizing db... ", end="")
    # lock.acquire()
    # db = connect_db()
    with open(schema_dir, mode='r') as f:
        db.cursor().executescript(f.read())
    db.commit()
    # lock.release()
    print("OK")

# def insert_db(db, query):
#     cur = db.execute(query)
#     # db.commit()
#     # db.close()
#     return cur.lastrowid

# def multi_insert_db(db, listOfQueries):
#     # db = db_connect()
#     # print(listOfQueries)
#     # db.execute("BEGIN")
#     for query in listOfQueries:
#         try:
#             db.execute(query)
#         except sqlite3.IntegrityError:
#             pass

def db_dump(fileName):
    db = db_connect()
    with open(fileName, 'w') as f:
        for line in db.iterdump():
            f.write('%s\n' % line)


### Network Data Queries

def networkInsert(payload):
    try:
        
        # reload_storm()
        client = payload["client"]
        ts = payload["ts"]
        # print("[DEBUG] received length trace:",len(request.form)) if args.verbose else None

        probesQueries = []

        db = db_connect()
        query = 'select src_addr, src_port, dst_addr, dst_port, ID, client from connections'
        connectionsInDb = {(str(res[0]), str(res[1]), str(res[2]), str(res[3])):(str(res[4]), str(res[5])) for res in db.execute(query).fetchall()}
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
                c = (str(src_host), str(src_port), str(dst_host), str(dst_port))

                if c in connectionsInDb and str(client) != connectionsInDb[c][1]: pass
                else:
                    if c not in connectionsInDb:
                        query = 'insert into connections (client, src_addr, src_port, dst_addr, dst_port) values (\'' + str(client) + '\',\'' + src_host + '\',\'' + src_port + '\',\'' + dst_host + '\',\'' + dst_port + '\')'
                        connId = db.execute(query).lastrowid
                        
                    else: connId = connectionsInDb[c][0]
                    
                    query = 'insert into probes (connection, ts, pkts, bytes) values (' + str(connId) + ',\'' + str(ts) + '\',\'' + pkts + '\',\'' + bts + '\')'
                    if query not in probesQueries: probesQueries.append(query)


        # print('length queries:', len(probesQueries))
        # multi_insert_db(db, probesQueries)
        for query in probesQueries:
            try:
                db.execute(query)
            except sqlite3.IntegrityError:
                print('sqlite3.IntegrityError:',query)

        db.commit()
        db.close()
                # dbNetworkInsert(client, ts, src_host,src_port,dst_host,dst_port, bts, pkts)
                
    except Exception as e:
        print ("[ERROR] : Wrong payload received in network insert")
        print(traceback.format_exc())

def dbPortInsert(client, port, pid, portsInDb):
    # p = getPort(db, client, port)
    if port not in portsInDb:
        query = 'insert into port_mapping (addr, port, pid) values (\'' + client + '\',\'' + port + '\',\'' + pid + '\')'
        # connId = insert_db(query)
        return query
    elif portsInDb[port] != pid: #update the database with the new pid (should happen only if the worker crashes)
        query = 'update port_mapping set pid = \'' + pid + '\' where addr == \'' + client + '\' and port == \'' + port + '\''
        # insert_db(query)
        return query
    return False

def portInsert(payload):
    try:
        # reload_storm()
        # print("here")
        client = payload["client"]
        # print("client:   ", client)
        # ts = time.time()

        portQueries = []
        db = db_connect()
        query = 'select port, pid from port_mapping where addr == \'' + client + '\''
        portsInDb = {res[0]:res[1] for res in db.execute(query).fetchall()}
        for key in payload:
            # print("new key:    ", key)
            if key != "client":
                port = str(key)
                pid = str(payload[key])
                # print(client,port,pid)
                query = dbPortInsert(client, port, pid, portsInDb)
                if query and query not in portQueries: portQueries.append(query)
        
        
        # multi_insert_db(db, portQueries)
        for query in portQueries:
            try:
                db.execute(query)
            except sqlite3.IntegrityError:
                print('sqlite3.IntegrityError:',query)

        db.commit()
        db.close()

    except Exception as e:
        print ("[ERROR] Wrong payload in port insert")
        print(traceback.format_exc())



### Storm DB managing

def updateStormDb(storm):
    newTopo = False
    workersChanged = False
    db = db_connect()

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
            newTopo = True
            query = 'INSERT INTO topology (ID, name) VALUES (\'' + t + '\',\'' + storm.topologies[t] + '\')'
            db.execute(query)
            # db.commit()

    #### update workers table
    query = 'SELECT host, port, topoID FROM worker'
    cur = db.execute(query)
    workersInDb = [(r[0],r[1],r[2]) for r in cur.fetchall()]
    for topo in storm.workers: ## worker[topoId] = [(host,port),(host,port),...]
        # if sorted(storm.workers[topo]) != sorted([(e[0],int(e[1])) for e in workersInDb if e[2] == topo]):
           
        for couple in storm.workers[topo]:
            if (couple[0], couple[1], topo) not in workersInDb:
                workersChanged = True
                query = 'INSERT INTO worker (host, port, topoID, last_seen) VALUES (\'' + couple[0] + '\',\'' + str(couple[1]) + '\',\'' + topo + '\',\'' + str(time.time()) +'\')'
                
            else:
                query = ' \
                UPDATE worker \
                SET last_seen = \'' + str(time.time()) + '\' \
                WHERE host = \'' + couple[0] + '\' AND port = \'' + couple[1] + '\' AND topoID = \'' + topo + '\''

            db.execute(query)
                # db.commit()
            
    #### update component table
    if newTopo:
        query = 'SELECT ID, topoID FROM component'
        cur = db.execute(query)
        componentsInDb = [(r[0],r[1]) for r in cur.fetchall()]
        for topo in storm.components: ## component[topoId] = [spout,spout,bolt,bolt,...]
            if sorted(storm.components[topo]) != sorted([e[0] for e in componentsInDb if e[1] == topo]):
                ### insert
                for c in storm.components[topo]:
                    if (c, topo) not in componentsInDb:
                        query = 'INSERT INTO component (ID, topoID) VALUES (\'' + c + '\',\'' + topo + '\')'
                        db.execute(query)
                    # db.commit()
    
    #### update executor table 

    if  workersChanged:

        query = 'SELECT executor, host, port, component FROM executor'
        cur = db.execute(query)
        executorsInDb = {r[3]:(r[0],r[1],r[2]) for r in cur.fetchall()}
        for topo in storm.executors: ## executor[topoId][component] = [(id,host,port),(id,host,port),...]
            for component in storm.executors[topo]:
                for e in storm.executors[topo][component]:
                    # if component not in executorsInDb:
                        ## insert
                    query = 'INSERT INTO executor (executor, time, host, port, component) ' + \
                            'VALUES (\'' + e[0] + '\',\'' + str(time.time()) + '\',\'' + e[1] + '\',\'' + str(e[2]) + '\',\'' + component + '\')'
                    # elif e[1] and (executorsInDb[component][1] != e[1] or executorsInDb[component][2] != e[2]):
                    #     ## update if something changed
                    #     query = 'UPDATE executor ' + \
                    #             'SET host = \'' + e[1] + '\' AND port = \'' + str(e[2]) + '\' ' + \
                    #             'WHERE executor = \'' + e[0] + '\' AND component = \'' + component + '\'' 
                    db.execute(query)
    
    db.commit()
    db.close()


"""
def checkStormDb(storm):
    print('Retreiving data from local DB... ', end='')
    ### follow storm.reload() scheme
    db = db_connect()
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



### Old Flask Queries

def getSummary():
    db = db_connect()
    query = 'SELECT src_addr, src_port, dst_addr, dst_port, SUM(pkts) as pkts, SUM(bytes) as bytes, MAX(ts) as ts \
            FROM connections, probes \
            WHERE connections.ID == probes.connection \
            GROUP BY probes.connection \
            ORDER BY bytes DESC;'
    cur = db.execute(query)
    return cur.fetchall()

def getTopoNetwork(addrs, ports):#, time):
    db = db_connect()
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

    db = db_connect()
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

    db = db_connect()
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
"""