from flask import Flask, jsonify
from flask import request

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
    return True

if __name__ == "__main__":
    app.run()

