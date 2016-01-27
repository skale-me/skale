# Skale

Skale is a real time distributed data processing system.

Skale provides the following modules:

* a fast messaging system: [skale-client](doc/skaleClient.md)
* a distributed data processing system: [skale-context](doc/skaleContext.md)

### Install
    npm install skale

### Start server
    bin/skale.js [port]

### Start workers
    bin/worker.js [-n number] [-H server_host] [-P server_port]

## Todo

* Topics for pub/sub
* Fault tolerant skale server
