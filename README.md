# netmonitor for Storm #

![last-tag](https://img.shields.io/github/tag-pre/ale93p/storm-netmonitor.svg?style=flat)
![python-version](https://img.shields.io/badge/python-3.5%2B-yellow.svg)

**netmonitor** is a distributed client-server tool for network metrics collection for Apache Storm [[1](http://storm.apache.org/)]

### Dependencies ###

* Python 3.x

* following modules on clients:
  
    * requests
    * xmlrpc
    * PyYAML: [here](https://github.com/yaml/pyyaml)

### Features ###

- [x] Conntrack [[2](http://conntrack-tools.netfilter.org/)] output parsing to retreive network metrics
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
