# Ugrid

Ugrid is a real time distributed data processing system.

Ugrid provides the following modules:

* a fast messaging system: [ugrid-client](doc/ugridClient.md)
* a distributed data processing system: [ugrid-context](doc/ugridContext.md)

### Install
    npm install ugrid

### Start server
    bin/ugrid.js [port]

### Start workers
    bin/worker.js [-n number] [-H server_host] [-P server_port]

## Todo

* Topics for pub/sub
* Fault tolerant ugrid server
