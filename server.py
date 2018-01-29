from flask import Flask, jsonify, request, abort
import argparse
import csv
import os.path
import time

app = Flask(__name__)
start_time = time.time()
filename = 'network_db_' + time.strftime("%d%m%y%H%M%s") + '.csv'

@app.route("/")
def intex():
    return "Index page of netmonitor"

@app.route("/test", methods=['GET'])
def test():
    var = request.args["var"]
    return jsonify({'sent' : var})

def writeToCsv(d):
    title_row = ['client', 'timestamp', 'snd_addr', 'snd_port', 'rcv_addr', 'rcv_port', 'pkts', 'bytes']
    if len(d) != len(title_row): raise Exception('Data lenght not matching')
    newfile = not os.path.isfile(filename)
    with open(filename, 'a', newline='') as csvfile:
        csvwriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        if newfile: csvwriter.writerow(title_row)
        csvwriter.writerow(d)

@app.route("/api/v0.1/network/insert", methods=['GET'])
def networkInsert():
    try:
        data = []

        data.append(request.remote_addr)

        data.append(request.args["ts"])

        data.append(request.args["src_host"])
        data.append(request.args["src_port"])

        data.append(request.args["dst_host"])
        data.append(request.args["dst_port"])

        data.append(request.args["pkts"])
        data.append(request.args["bytes"])
    
    except:
        print ("[ERROR] Wrong API request")
        abort(400)

    print ("[VERBOSE] Received from client:",data) if args.verbose else None
    
    try:
        writeToCsv(data)
    except Exception as e:
        print(e)
        abort(400)

    print ("[VERBOSE] Data write success") if args.verbose else None
    return "Ok"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter, description="Start netmonitor server")
    parser.add_argument("-p", "--port", dest="port", type=int, help="specify listening port", default=5000)
    parser.add_argument("-v", "--verbose", dest="verbose", help="verbose mode", action="store_true", default=False)
    args = parser.parse_args()

    print(" * Enabled verbose output * ") if args.verbose else None

    app.run(host='0.0.0.0', port=args.port)
