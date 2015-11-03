<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Ugrid](#ugrid)
        - [Install](#install)
        - [Start server](#start-server)
        - [Start workers](#start-workers)
    - [Todo](#todo)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

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
