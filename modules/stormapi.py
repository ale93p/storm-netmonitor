# REsT requests implementation to call Apache Storm APIs
import json
import requests

class stormCollector():
    def __init__(self, api_addr, api_port = 8080):
        self.address = api_addr
        self.port = api_port
        self.baseUrl = 'http://' + str(api_addr) + ':' + str(api_port) + '/api/v1'
        
        self.topoUrl = self.baseUrl + '/topology'
        self.summUrl = self.topoUrl + '/summary'
        self.topologies = {}
        self.workers = {}
        self.components = {}
        self.executors = {}
        self.reload()

    def reload(self):
        self.topologies = self.getTopologyList()
        if len(self.topologies) > 0:
            for topoId in self.getTopologyIds():
                self.workers[topoId] = self.getTopologyWorkers(topoId)
                self.components[topoId] = self.getTopologyComponents(topoId) 
                self.executors[topoId] = self.getTopologyExecutors(topoId)

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

    def getMetrics(baseUrl, topoId):
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