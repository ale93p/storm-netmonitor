import requests
import json

url = "http://127.0.0.1:5000/api/v0.1/insert"



var = requests.get(url + "?src_host=sdn1)

print var.json()