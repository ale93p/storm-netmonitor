# REsT requests implementation to call Apache Storm APIs
import time, json, requests
from socket import gethostbyname
from subprocess import DEVNULL, STDOUT, check_call

class StormCollector():
    def __init__(self, api_addr = None, api_port = 8080):
        self.address = api_addr
        self.port = api_port
        
        self.connected = False
        self.lastConnected = 0
        self.lastUpdate = 0

        self.baseUrl = 'http://' + str(api_addr) + ':' + str(api_port) + '/api/v1'
        self.topoUrl = self.baseUrl + '/topology'
        self.summUrl = self.topoUrl + '/summary'
        self.superUrl = self.baseUrl + '/supervisor/summary'

        self.supervisors = []
        self.topologies = {}
        self.workers = {}
        self.components = {}
        self.executors = {}

        #self.reload()

    def isConnected(self):
        
        now = time.time()
        if now - self.lastConnected > 300 or not self.connected:
            print("Checking Storm Connection... ", end="",flush=True)
            self.lastConnected = now
            try:
                if check_call(['ping','-c1','-W1000',self.address], stdout=DEVNULL, stderr=STDOUT) is 0:
                    self.connected = True
                    print("OK")
                    return True
            except:
                self.connected = False
                print("ERROR")
                return False
        else: return True

    def reload(self):
        all_updated = True
        if self.isConnected():
            self.supervisors = self.getStormSupervisors()
            if self.supervisors == -1: self.connected = False
            self.topologies = self.getTopologyList()
            if len(self.topologies) > 0:
                for topoId in self.getTopologyIds():
                    self.workers[topoId] = self.getTopologyWorkers(topoId)
                    if self.workers[topoId]:
                        self.components[topoId] = self.getTopologyComponents(topoId) 
                        if self.components[topoId]:
                            self.executors[topoId] = self.getTopologyExecutors(topoId)
                            
                    if not topoId in self.executors: all_updated = False 
                

                
                if all_updated:
                    self.lastUpdate = time.time()
                    print("[STORM-API] Updated at ",self.lastUpdate)
                    return True
                else:
                    return False

    def getTopologyList(self):
        url = self.summUrl
        topologies = {}
        topoSummary = requests.get(url)
        jsonData = topoSummary.json()
        for topo in jsonData["topologies"]:
            topologies[topo['id']] = topo['name']
        return topologies

    def getTopologyIds(self):
        topos = self.topologies
        ids = []
        for key in topos:
            ids.append(key)
        return ids
    
    def getTopologyName(self, topoId):
        return self.topologies[topoId]
   
    def getStormSupervisors(self):
        # print(self.superUrl)
        try:
          res = requests.get(self.superUrl)
        except requests.exceptions.ConnectionError:
            return -1
        jsonData = res.json()
        
        supervisors = []
        try:
            for supervisor in jsonData["supervisors"]:
                supervisors.append((supervisor["host"],supervisor["uptimeSeconds"]))
        except:
            return None
        return supervisors

    def getLastUp(self, id):
        url = self.topoUrl + '/' + id
        times = []

        res = requests.get(url)
        jsonData = res.json()
        if "workers" in jsonData:
            for worker in jsonData["workers"]:
                times.append(worker["uptimeSeconds"])

            if times: return min(times)
            else: return 0
        else: return None

    
    def getTopologyWorkers(self, topoId):
        """ Returns empty list if no topology found """
        url = self.baseUrl + "/topology-workers/" + topoId
        topoWorkers = []
        res = requests.get(url)
        jsonData = res.json()
        try:
            for work in jsonData["hostPortList"]:
                topoWorkers.append((work["host"], work["port"]))
        except:
            return None

        return topoWorkers

    def getWorkersAddr(self, topoId):
        addr = []
        for worker in self.workers[topoId]:
            ip = self.getIpByName(worker[0])
            if ip not in addr: addr.append(ip)
        addr.append('127.0.0.1')
        addr.append('127.0.1.1')
        return tuple(addr)
    
    def getWorkersPort(self, topoId):
        ports = []
        for worker in self.workers[topoId]:
            port = worker[1]
            if port not in ports: ports.append(str(port))
        return tuple(ports)

    def getTopologyComponents(self, topoId):
        """ Returns empty list if no topology found """
        url = self.topoUrl + "/" + topoId + "/metrics"
        topoComponents = []
        res = requests.get(url)
        jsonData = res.json()
        try:
            for spout in jsonData["spouts"]:
                topoComponents.append(spout["id"])
            for bolt in jsonData["bolts"]:
                topoComponents.append(bolt["id"])
        except:
            return None

        return topoComponents

    def getTopologyExecutors(self, topoId):
        '''Returns list of all the executors in the topology'''
        topoExecutors = {}
        for c in self.components[topoId]:
            url = self.topoUrl + "/" + topoId + '/component/' + c
            res = requests.get(url)
            jsonData = res.json()
            topoExecutors[c] = []
            for e in jsonData["executorStats"]:
                topoExecutors[c].append((e["id"], e["host"], e["port"]))
        
        return topoExecutors
    
    def executorsLength(self, executors):
        w = 0
        for c in executors:
            w += len(executors[c])
        return w
    
    def getNameByIp(self, ip):
        for node in self.supervisors:
            if gethostbyname(node[0]) == ip: return node[0]
        return None
    
    def getIpByName(self, name):
            return gethostbyname(name)

    def getMetrics(self, baseUrl, topoId):
        """Returns number of tuples executed so far"""
        executorValues = {}
        
        for c in self.components[topoId]:
            #print(c)
            url = self.topoUrl + "/" + topoId + '/component/' + c
            #print(url)
            componentDetails = requests.get(url)
            jsonData = componentDetails.json()
            #print jsonData
            for e in jsonData["executorStats"]:
                componentsValues[e["id"]] = e["emitted"]