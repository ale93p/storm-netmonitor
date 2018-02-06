# tcpprobe parser module

class ProbeParser:
    def __init__(self, probe):
        splitted = probe.split()
        self.ts = float(splitted[0])             # Time in seconds

        ###
        # Apparently tcp_probe mess up with sender receiver and packet size 
        # basically, it shows the sender and receiver for the reply packet (outgoing)
        # but it shows the "byte in packet" values of the received packet (incoming)
        # inverting source and destination should fix the problem
        #
        #self.sh = self.getAddress(splitted[1])   # Source address
        #self.sp = self.getPort(splitted[1])      # Source port
        #self.dh = self.getAddress(splitted[2])   # Destination address
        #self.dp = self.getPort(splitted[2])      # Destination port
        #
        self.sh = self.getAddress(splitted[2])
        self.sp = self.getPort(splitted[2])
        self.dh = self.getAddress(splitted[1])
        self.dp = self.getPort(splitted[1])
        ###

        self.by = int(splitted[3])               # Bytes in packet
        self.ns = splitted[4]                    # Next send sequence nbr
        self.us = splitted[5]                    # Unacknowledged sequence nbr
        self.cw = int(splitted[6])               # Congestion window
        self.ss = int(splitted[7])               # Slow start threshold
        self.sw = int(splitted[8])               # Send window
    
    def getAddress(self, app): #app: address plus port
        return app.rsplit(':',1)[0].rsplit(':',1)[-1][:-1]

    def getPort(self, app):
        return int(app.rsplit(":",1)[1])

class ProbeAggregator:
    def __init__(self):
        self.pkts = 0
        self.bytes = 0

    def addPacket(self, by):
        self.pkts += 1
        self.bytes += by

    def reset(self):
        self.pkts = 0
        self.bytes = 0