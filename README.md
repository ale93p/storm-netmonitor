# netmonitor for Storm#

netmonitor for Storm helps you to collect network statistics from your Storm cluster

### Dependencies ###

* Python 3.x
* [Flask](http://flask.pocoo.org/) on server
```
sudo pip3 install flask
```
* `requests` module on client
```
sudo pip3 install requests
```

### Features ###

* Server prints received data
* Client sends dummy data

### How to run it ###

#### Server ####
Go to netmonitor folder and run:
```
python3 server.py
```
#### Clients ####
_on each client_
First, [tcpprobe](https://wiki.linuxfoundation.org/networking/tcpprobe) have to be configured on(as _sudo_):
```
# mod_probe -r tcp_probe
# mod_prbe tcp_probe port=0 full=1
```
Then, go to netmonitor folder and run:
```
python3 client.py server_address
```
* or you can access it from the web, through REST APIs