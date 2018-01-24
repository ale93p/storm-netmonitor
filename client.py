import requests
import json
import time
from optparse import OptionParser

def networkInsert(ts, sh, sp, dh, dp, pk, by):
    url = "http://127.0.0.1:5000/api/v0.1/insert"
    return requests.get(url + "?ts=" + str(ts) + "src_host=" + str(sh) + "&src_port=" + str(sp) + "&dst_host=" + str(dh) + "&dst_port=" + str(dp) + "&pkts=" + str(pk) + "&bytes=" + str(by))
    
if __name__ == "__main__":
    usage = "usage: %prog -s SERVER_ADDR [-p]"
    parser = OptionParser(usage=usage)
    parser.add_option("-s", "--server", dest="server_addr", help="specify server address")
    parser.add_option("-p", "--port", dest="server_port", help="specify server listening port", default="5000")
    (options, args) = parser.parse_args()

    if not options.server_addr:
        print("You must specify a server address")

    while True:
        print(networkInsert(time.time(),"dummy1",1234,"dummy2",4321,15,3287))
        time.sleep(5)