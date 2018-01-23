from flask import Flask
from flask import request

app = Flask(__name__)

@app.route("/")
def intex():
    return "Index page of netmonitor"

@app.route("/test", methods=['GET'])
def test():
    print request.args["var"]
    return True



if __name__ == "__main__":
    app.run()

