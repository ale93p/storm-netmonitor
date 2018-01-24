# tcpprobe parser module

class ProbeParser:
    def __init__(self, probe):
       splitted = probe.split()
       self.ts = float(splitted[0])
       self.sh = self.getAddress(splitted[1])
       self.sp = self.getPort(splitted[1])
       self.sh = self.getAddress(splitted[2])
       self.dp = self.getPort(splitted[2])
       self.by = int(splitted[3])
    
    def getAddress(self, app): #app: address plus port
        return app.rsplit(":",1)[0]

    def getPort(self, app):
        return int(app.rsplit(":",1)[1])