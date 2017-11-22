#!/usr/bin/env node

// Copyright 2016 Luca-SAS, licensed under the Apache License 2.0

// Todo:
// - record/replay input messages
// - handle foreign messages
// - statistics in monitoring
// - topics permissions (who can publish / subscribe)

'use strict';

const child_process = require('child_process');
const fs = require('fs');
const net = require('net');
const os = require('os');
const util = require('util');
const stream = require('stream');
const uuidGen = require('uuid');
const SkaleClient = require('../lib/client.js');
const webSocketServer = require('ws').Server;
const websocket = require('websocket-stream');

const workerControllers = [];
const pendingMasters = [];
let workerStock = [];
let wsid = 1; // worker stock id
let expectedWorkers = 0;  // number of expected workers per stock

const opt = require('node-getopt').create([
  ['h', 'help', 'print this help text'],
  ['H', 'Host=ARG', 'primary server host (default none)'],
  ['l', 'local=ARG', 'start local worker controller (default ncpu workers)'],
  ['m', 'memory=ARG', 'set max memory in MB for workers in local mode'],
  ['M', 'MyHost=ARG', 'advertised hostname'],
  ['N', 'Name=ARG', 'advertised server name (default localhost)'],
  ['P', 'Port=ARG', 'primary server port (default none)'],
  ['p', 'port=ARG', 'server port (default 12346)'],
  ['s', 'slow', 'disable peer-to-peer file transfers though HTTP'],
  ['w', 'wsport=ARG', 'listen on websocket port (default none)'],
  ['V', 'version', 'print version']
]).bindHelp().parseSystem();

if (opt.options.version) {
  const pkg = require('../package');
  return console.log(pkg.name + '-' +  pkg.version);
}

const clients = {};
let clientNum = 1;
const clientMax = SkaleClient.minMulticast;
const minMulticast = SkaleClient.minMulticast;
const topics = {};
let topicNum = -1;
const UInt32Max = 4294967296;
const topicMax = UInt32Max - minMulticast;
const topicIndex = {};
const memory = opt.options.memory || process.env.SKALE_MEMORY || 0;
//var name = opt.options.name || 'localhost';   // Unused until FT comes back
const hostname = opt.options.MyHost || os.hostname();
const port = Number(opt.options.port) || 12346;
const wsport = opt.options.wsport || port + 2;
const crossbar = {};
const nworker = (opt.options.local > 0) ? opt.options.local : 0;
const access = process.env.SKALE_KEY;
const stats = {
  masters: 0,
  workerControllers: 0,
  workers: 0,
  workerHosts: {}
};

process.title = 'skale-server ' + port;

function SwitchBoard(sock) {
  if (!(this instanceof SwitchBoard))
    return new SwitchBoard(sock);
  stream.Transform.call(this, {objectMode: true});
  sock.index = getClientNumber();
  crossbar[sock.index] = sock;
  this.sock = sock;
}
util.inherits(SwitchBoard, stream.Transform);

SwitchBoard.prototype._transform = function (chunk, encoding, done) {
  const to = chunk.readUInt32LE(0, true);
  let o = {};
  if (to >= minMulticast) { // Multicast
    const sub = topics[to - minMulticast].sub;
    let len = sub.length;
    let n = 0;

    if (len === 0) return done();
    for (let i in sub) {
      // Flow control: adjust to the slowest receiver
      if (crossbar[sub[i]]) {
        crossbar[sub[i]].write(chunk, function () {
          if (++n === len) done();
        });
      } else if (--len === 0) done();
    }
  } else if (to > 1) {  // Unicast
    if (crossbar[to]) {
      crossbar[to].write(chunk, done);
    } else {
      console.error('# Unknown destination', to);
      done();
    }
  } else if (to === 1) {  // Foreign (to be done)
  } else if (to === 0) {  // Server request
    try {
      o = JSON.parse(chunk.slice(8));
    } catch (error) {
      console.error(error);
      return done();
    }
    if (!(o.cmd in clientRequest)) {
      o.error = 'Invalid command: ' + o.cmd;
      o.cmd = 'reply';
      this.sock.write(SkaleClient.encode(o), done);
      console.error('# clientRequest error:', o.error);
    } else if (clientRequest[o.cmd](this.sock, o)) {
      o.cmd = 'reply';
      this.sock.write(SkaleClient.encode(o), done);
    } else done();
  }
};

// Client requests functions, return true if a response must be sent
// to client, false otherwise. Reply data, if any,  must be set in msg.data.
const clientRequest = {
  connect: function (sock, msg) {
    let ret = true;
    if (access && msg.access !== access) {
      console.log('## Skale connect failed: access denied');
      msg.error = 'access denied, check SKALE_KEY';
      return true;
    }
    register(null, msg, sock);
    if (msg.data.query) msg.data.devices = devices(msg);
    if (msg.data.notify in clients && clients[msg.data.notify].sock) {
      clients[msg.data.notify].sock.write(SkaleClient.encode({cmd: 'notify', data: msg.data}));
      clients[msg.data.notify].closeListeners[msg.data.uuid] = true;
    }
    switch (msg.data.type) {
    case 'worker-controller':
      msg.data.wsid = wsid;
      expectedWorkers += msg.data.nworkers;
      workerControllers.push(msg.data);
      stats.workerControllers++;
      stats.workerHosts[msg.data.id] = {
        hostname: msg.data.hostname,
        nworkers: msg.data.nworkers,
        ncpus: msg.data.ncpus,
        memory: msg.data.memory,
        platform: msg.data.platform,
        arch: msg.data.arch,
        cpumodel: msg.data.cpumodel,
        cpuspeed: msg.data.cpuspeed
      };
      break;
    case 'worker':
      if (wsid === msg.data.wsid) {
        workerStock.push(msg.data);
        if (pendingMasters.length && workerStock.length >= expectedWorkers) {
          const master = pendingMasters.shift();
          master.data.devices = workerStock;
          master.cmd = 'reply';
          if (clients[master.data.uuid].sock)
            clients[master.data.uuid].sock.write(SkaleClient.encode(master));
          postMaster(master.data.uuid);
        }
      }
      stats.workers++;
      break;
    case 'master':
      if (expectedWorkers && workerStock.length >= expectedWorkers) {
        msg.data.devices = workerStock;
        postMaster(msg.data.uuid);
      } else {
        pendingMasters.push(msg);
        ret = false;
      }
      stats.masters++;
      break;
    }
    console.log('## Connect', msg.data.type, msg.data.id, msg.data.uuid);
    return ret;

    function postMaster(muuid) {
      // Setup notifications to terminate workers on master end
      for (let i = 0; i < workerStock.length; i++) {
        const wuuid = workerStock[i].uuid;
        clients[muuid].closeListeners[wuuid] = true;
        clients[wuuid].closeListeners[muuid] = true;
      }
    }
  },
  devices: function (sock, msg) {
    msg.ufrom = sock.client.uuid;
    msg.data = devices(msg);
    return true;
  },
  end: function (sock) {
    sock.client.end = true;
    return false;
  },
  get: function (sock, msg) {
    msg.data = clients[msg.data] ? clients[msg.data].data : null;
    return true;
  },
  id: function (sock, msg) {
    msg.data = msg.data in clients ? clients[msg.data].index : null;
    return true;
  },
  notify: function (sock, msg) {
    if (clients[msg.data])
      clients[msg.data].closeListeners[sock.client.uuid] = true;
    return false;
  },
  set: function (sock, msg) {
    if (typeof msg.data !== 'object') return false;
    for (let i in msg.data)
      sock.client.data[i] = msg.data[i];
    pubmon({event: 'set', uuid: sock.client.uuid, data: msg.data});
    return false;
  },
  subscribe: function (sock, msg) {
    subscribe(sock.client, msg.data);
    return false;
  },
  tid: function (sock, msg) {
    const topic = msg.data;
    const n = msg.data = getTopicId(topic);
    // First to publish becomes topic owner
    if (!topics[n].owner) {
      topics[n].owner = sock.client;
      sock.client.topics[n] = true;
    }
    return true;
  },
  unsubscribe: function (sock, msg) {
    unsubscribe(sock.client, msg.data);
    return false;
  }
};

function startWorkerStock() {
  // Pre-fork new workers to renew the stock
  wsid++;
  workerStock = [];
  for (let i = 0; i < workerControllers.length; i++) {
    clients[workerControllers[i].uuid].sock.write(SkaleClient.encode({
      cmd: 'getWorker',
      wsid: wsid,
      n: workerControllers[i].nworkers
    }));
  }
}

// Create a source stream and topic for monitoring info publishing
const mstream = new SwitchBoard({});
const monid =  getTopicId('monitoring') + minMulticast;
function pubmon(data) {
  mstream.write(SkaleClient.encode({cmd: 'monitoring', id: monid, data: data}));
}

process.on('uncaughtException', function uncaughtException(err) {
  console.error(err.stack);
});

process.on('SIGTERM', function sigterm() {
  console.error('terminated, exit');
  process.exit();
});

console.log('## Started', Date());
// Start a TCP server
if (port) {
  net.createServer(handleConnect).listen(port);
  console.log('## Listening TCP on', port);
}

// Start a websocket server if a listening port is specified on command line
if (wsport) {
  console.log('## Listening WebSocket on', wsport);
  const wss = new webSocketServer({port: wsport});
  wss.on('connection', function (ws) {
    const sock = websocket(ws);
    sock.ws = true;
    handleConnect(sock);
    // Catch error/close at websocket level in addition to stream level
    ws.on('close', function () {
      handleClose(sock);
    });
    ws.on('error', function (error) {
      console.log('## websocket connection error', error.stack);
      handleClose(sock);
    });
  });
}

// Start local workers if required
if (opt.options.local) startWorker();

function startWorker() {
  let args = ['-P', port, '-n', nworker, '-m', memory];
  args = args.concat(opt.options.slow ? ['-s'] : ['-M', hostname]);
  const worker = child_process.fork( __dirname + '/worker.js', args, {
    execArgv: [ '--expose-gc' ],
    stdio: 'inherit'
  });
  worker.on('close', startWorker);
}

// update statistics every 3s
fs.mkdir('/tmp/skale', function () {});
setInterval(function () {
  fs.writeFile('/tmp/skale/server-stats', JSON.stringify(stats), function () {});
}, 3000);

function handleClose(sock) {
  const cli = sock.client;
  if (cli) {
    console.log('## Close:', cli.data.type, cli.index, cli.uuid);
    pubmon({event: 'disconnect', uuid: cli.uuid});
    cli.sock = null;
    switch (cli.data.type) {
    case 'worker-controller':
      // Resize stock capacity
      expectedWorkers -= cli.data.nworkers;
      for (let i = 0; i < workerControllers.length; i++) {
        if (cli.uuid === workerControllers[i].uuid) {
          delete stats.workerHosts[workerControllers[i].id];
          workerControllers.splice(i, 1);
        }
      }
      stats.workerControllers--;
      break;
    case 'worker':
      // Remove worker from stock
      for (let i = 0; i < workerStock.length; i++) {
        if (cli.uuid === workerStock[i].uuid)
          workerStock.splice(i, 1);
      }
      stats.workers--;
      break;
    case 'master':
      // Remove master from pending masters, avoiding future useless workers start
      for (let i in pendingMasters) {
        if (pendingMasters[i].data.uuid === cli.uuid) {
          pendingMasters.splice(i, 1);
          break;
        }
      }
      stats.masters--;
      startWorkerStock();
      break;
    }
    for (let i in cli.closeListeners) {
      if (i in clients && clients[i].sock)
        clients[i].sock.write(SkaleClient.encode({cmd: 'remoteClose', data: cli.uuid}));
    }
    for (let i in cli.topics) {   // Remove owned topics
      delete topicIndex[topics[i].name];
      delete topics[i];
    }
    if (cli.end) delete clients[cli.uuid];
  } else {
    console.log('## Close:', sock._peername);
  }
  if (sock.index) delete crossbar[sock.index];
  sock.removeAllListeners();
}

function handleConnect(sock) {
  if (sock.ws) {
    console.log('## Connect websocket from', sock.socket.upgradeReq.headers.origin);
  } else {
    console.log('## Connect tcp', sock.remoteAddress, sock.remotePort);
    sock.setNoDelay();
  }
  sock.on('end', function () {
    handleClose(sock);
  });
  sock.on('error', function sockError(error) {
    console.log('## connection error', error.stack);
    handleClose(sock);
  });
  sock.pipe(new SkaleClient.FromGrid()).pipe(new SwitchBoard(sock));
}

function getClientNumber() {
  let n = 100000;
  do {
    clientNum = (clientNum < clientMax) ? clientNum + 1 : 2;
  } while (clientNum in crossbar && --n);
  if (!n) throw new Error('getClientNumber failed');
  return clientNum;
}

function register(from, msg, sock)
{
  const uuid = msg.uuid || uuidGen.v1();
  sock.client = clients[uuid] = {
    index: sock.index,
    uuid: uuid,
    owner: from ? from : uuid,
    data: msg.data || {},
    sock: sock,
    topics: {},
    closeListeners: {}
  };
  pubmon({event: 'connect', uuid: uuid, data: msg.data});
  //msg.data = {uuid: uuid, token: 0, id: sock.index};
  if (!msg.data) msg.data = {};
  msg.data.uuid = uuid;
  msg.data.token = 0;
  msg.data.id = sock.index;
}

function devices(msg) {
  const query = msg.data.query;
  const result = [];

  for (let i in clients) {
    let match;
    if (!clients[i].sock) continue;
    match = true;
    for (let j in query) {
      if (!clients[i].data || clients[i].data[j] !== query[j]) {
        match = false;
        break;
      }
    }
    if (match) {
      result.push({
        uuid: i,
        id: clients[i].index,
        ip: clients[i].sock.remoteAddress,
        data: clients[i].data
      });
    }
  }
  return result;
}

function getTopicId(topic) {
  if (topic in topicIndex) return topicIndex[topic];
  let n = 10000;
  do {
    topicNum = (topicNum < topicMax) ? topicNum + 1 : 0;
  } while (topicNum in topics && --n);
  if (!n) throw new Error('getTopicId failed');
  topics[topicNum] = {name: topic, id: topicNum, sub: []};
  topicIndex[topic] = topicNum;
  return topicIndex[topic];
}

function subscribe(client, topic) {
  const sub = topics[getTopicId(topic)].sub;
  if (sub.indexOf(client.index) < 0)
    sub.push(client.index);
}

function unsubscribe(client, topic) {
  if (!(topic in topicIndex)) return;
  const sub = topics[topicIndex[topic]].sub;
  const i = sub.indexOf(client.index);
  if (i >= 0) sub.splice(i, 1);
}
