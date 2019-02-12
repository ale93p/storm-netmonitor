# netmonitor for Storm #

netmonitor for Storm helps you to collect network statistics from your Storm cluster

### Dependencies ###

* Python 3.x

* following modules on clients:
  
    * requests
    * xmlrpc
    * PyYAML: [here](https://github.com/yaml/pyyaml)

### Features ###

- [x] Conntrack output parsing to retreive network metrics
- [x] In-memory SQLite3 to store metrics
- [x] XMLRCP module to send and aggregate results on server

### How to run it ###

#### Server side:

```
$ python3 server.py --initdb --nimbus ${nimbus.address}
```

#### Client side:    
```
$ python3 client.py ${server.address}
```