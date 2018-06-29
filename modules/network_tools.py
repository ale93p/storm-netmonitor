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
    def __init__(self, pkts = 0, bts = 0):
        self.pkts = pkts
        self.bytes = bts

    def addPacket(self, by):
        self.pkts += 1
        self.bytes += by

    def reset(self):
        self.pkts = 0
        self.bytes = 0

class ConntrackParser:
    ### http://conntrack-tools.netfilter.org/manual.html#requirements
    ### /bin/echo "1" > /proc/sys/net/netfilter/nf_conntrack_acct

    def __init__(self, ports):
        self.ports = ports
    
    def parseOutput(self, output):
        out_lines = self.splitOutput(output)
        out_dict = self.createDictionary(out_lines)
        # tcp_connections = filterByProtocol(out_lines, 'tcp')
        filtered_connections = self.filterByPorts(out_dict, self.ports)
        return filtered_connections

    def splitOutput(self, output):
        out_lines = output.decode().split('\n')
        for i in range(len(out_lines)):
            out_lines[i] = out_lines[i].split()
        return out_lines

    def createDictionary(self, lines):
        out_dict = []
        for line in lines:
            line_dict = {}
            sec_line_dict = {}
            try:
                if len(line) > 1:
                    line_dict['protocol'] = line[0]
                    for field in line:
                        if '=' in field:
                            split = field.split('=')
                            if(split[0] not in line_dict): line_dict[split[0]] = split[1]
                            else: sec_line_dict[split[0]] = split[1]
                    out_dict.append(line_dict)
                    if(sec_line_dict): 
                        sec_line_dict['protocol'] = line[0]
                        out_dict.append(sec_line_dict)
            except Exception as e:
                # print(traceback.format_exc())
                pass
        return out_dict

    def filterByPorts(self, connections_dict, ports):
        filtered = []
        for connection in connections_dict:
            if ('sport' in connection and int(connection['sport']) in ports) or ('dport' in connection and int(connection['dport']) in ports):
                filtered.append(connection)
        return filtered