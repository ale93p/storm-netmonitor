from flask import Flask
app = Flask(__name__)

@app.route("/write", methods = ['GET'])
def write():
    return "Hello World!"




if __name__ == "__main__":
    app.run()

