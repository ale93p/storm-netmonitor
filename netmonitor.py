from flask import Flask
from flask import request

app = Flask(__name__)

@app.route("/")
def intex():
    return "Index page of netmonitor"

@app.route("/test", methods=['GET'])
def test():
    return request.args["var"]



if __name__ == "__main__":
    app.run()

