# netmonitor for Storm #

netmonitor for Storm helps you to collect network statistics from your Storm cluster

### Dependencies ###

* Python 3.x
* [Flask](http://flask.pocoo.org/) on server:
 
     ```
     $ sudo pip3 install flask
     ```

* `requests` module on client:

     ```
     $ sudo pip3 install requests
     ```

### Features ###

#### tcpprobe module ####

The module `tcpprobe.py` is used to manage data collected through [tcp_probe](https://wiki.linuxfoundation.org/networking/tcpprobe), the module features:

- a parser: to read and make usable the data in a packet line of tcp_probe
- an aggregator: to aggregate values for a specific TCP connection (integrating with dictionaries): total amount of packets and total amount data in those packets

#### Client-Server Architecture ####

* The clients obtain data through tcpprobe, aggregates it, and sends it to the server each 10 seconds
* The server receives data from the clients, through ReST APIs, and store them in a local SQLite database (v0.2.0+)

### How to run it ###

#### Server ####
Go to netmonitor folder and run:

```
$ python3 server.py
```

#### Clients ####

_on each client_:

1. First, tcp_probe have to be configured on(as *_sudo_*):
    
```
# modprobe -r tcp_probe
# modprobe tcp_probe port=0 full=1
# chmod 444 /proc/net/tcpprobe
```

2. Then, go to netmonitor folder and run:
    
    ```
    $ python3 client.py server_address
    ```