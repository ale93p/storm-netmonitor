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
    var = request.args["var"]
    print var
    return "Ok"

if __name__ == "__main__":
    app.run()

