from flask import Flask, jsonify
from flask import request
import argparse

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
        print ("[ERROR] Wrong API request")
        return "ERROR"

    print ("[DEBUG] Received from client: (",ts," ",src_host,":",src_port,"->",dst_host,":",dst_port," ",pkts,"pkts ",bts,"Bytes)")
    return "Ok"

if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter, description="Start netmonitor server")
    parser.add_argument("-p", "--port", dest="port", type=int, help="specify listening port", default=5000)
    args = parser.parse_args()

    app.run(host='0.0.0.0', port=args.port)
