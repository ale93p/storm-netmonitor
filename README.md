# netmonitor for Storm #

netmonitor for Storm helps you to collect network statistics from your Storm cluster

### Dependencies ###

* Python 3.x
* [Flask](http://flask.pocoo.org/) on server:
 
     ```
     $ sudo pip3 install flask
     ```

* following modules on client:
  
    * requests: `$ sudo pip3 install requests`
    
    * psutil: `$ sudo pip3 install psutil`
  
    * PyYAML: [here](https://github.com/yaml/pyyaml)

### Features ###

#### tcpprobe module ####

The module `tcpprobe.py` is used to manage data collected through [tcp_probe](https://wiki.linuxfoundation.org/networking/tcpprobe), the module features:

- a parser: to read and make usable the data in a packet line of tcp_probe
- an aggregator: to aggregate values for a specific TCP connection (integrating with dictionaries): total amount of packets and total amount data in those packets

### stormapi module ###

Set of functions used to retrieve informations from the storm cluster APIs in a custom storm class. 
(postmortem update) This data is then stored in the database for further analysis.

#### Client-Server Architecture ####

* The clients obtain data through tcpprobe, aggregates it, and sends it to the server each 10 seconds
* The server receives data from the clients, through ReST APIs, and store them in a local SQLite database (v0.2.0+): network data, storm cluster informations (from postmortem update)

### How to run it ###

#### Server ####
Go to netmonitor folder and run:

```
$ python3 server.py
```

#### Clients ####

_on each client_:

* **MODPROBE CONFIGURATION**: First, tcp_probe has to be configured on(as *_sudo_*):
    
    ```
    # modprobe -r tcp_probe
    # modprobe tcp_probe port=0 full=1
    # chmod 444 /proc/net/tcpprobe
    ```

* Then, go to netmonitor folder and run:
    
    ```
    $ python3 client.py server_address
    ```