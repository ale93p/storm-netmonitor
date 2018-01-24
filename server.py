from flask import Flask, jsonify
from flask import request
from optparse import OptionParser

app = Flask(__name__)

@app.route("/")
def intex():
    return "Index page of netmonitor"

@app.route("/test", methods=['GET'])
def test():
    var = request.args["var"]
    return jsonify({'sent' : var})

@app.route("/api/v0.1/network/insert", methods=['GET'])
def networkInsert():
    try:
        ts = request.args["ts"]

        src_host = request.args["src_host"]
        src_port = request.args["src_port"]

        dst_host = request.args["dst_host"]
        dst_port = request.args["dst_port"]

        pkts = request.args["pkts"]
        bts = request.args["bytes"]
    
    except:
        return "ERROR"

    print ("[DEBUG] Received from client: (",ts," ",src_host,":",src_port,"->",dst_host,":",dst_port," ",pkts,"pkts ",bts,"Bytes)")
    return "Ok"

if __name__ == "__main__":
    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option("-p", "--port", dest="port", help="specify listening port")
    (options, args) = parser.parse_args()

    if not options.port:
        app.run(host='0.0.0.0')
    else:
        app.run(host='0.0.0.0', port=int(options.port))
