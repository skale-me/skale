#!/usr/bin/env node

// Copyright 2016 Luca-SAS, licensed under the Apache License 2.0

'use strict';

const child_process = require('child_process');
const fs = require('fs');
const os = require('os');
const cluster = require('cluster');
const url = require('url');
const zlib = require('zlib');
const stream = require('stream');

const mkdirp = require('mkdirp');
const uuid = require('uuid');
const S3 = require('aws-sdk/clients/s3');
const azure = require('azure-storage');
const parquet = require('../lib/stub-parquet.js');

const SkaleClient = require('../lib/client.js');
const Dataset = require('../lib/dataset.js');
const Task = require('../lib/task.js');
const Lines = require('../lib/lines.js');
const sizeOf = require('../lib/rough-sizeof.js');
const readSplit = require('../lib/readsplit.js').readSplit;

const opt = require('node-getopt').create([
  ['h', 'help', 'print this help text'],
  ['d', 'debug', 'print debug traces'],
  ['m', 'memory=ARG', 'set max memory in MB for workers'],
  ['M', 'MyHost=ARG', 'advertised hostname (peer-to-peer)'],
  ['n', 'nworker=ARG', 'number of workers (default: number of cpus)'],
  ['r', 'retry=ARG', 'number of connection retry (default 0)'],
  ['s', 'slow', 'disable peer-to-peer file transfers though HTTP'],
  ['G', 'forcegc', 'workers force garbage collect at end of task'],
  ['H', 'Host=ARG', 'server hostname (default localhost)'],
  ['P', 'Port=ARG', 'server port (default 12346)'],
  ['V', 'version', 'print version']
]).bindHelp().parseSystem();

if (opt.options.version) {
  const pkg = require('../package');
  return console.log(pkg.name + '-' +  pkg.version);
}

const debug = opt.options.debug || false;
const forceGc = opt.options.forcegc || false;
const nworkers = +opt.options.nworker || +(process.env.SKALE_WORKER_PER_HOST ? process.env.SKALE_WORKER_PER_HOST : os.cpus().length);
const memory = +(opt.options.memory || process.env.SKALE_MEMORY);
const mm = new MemoryManager(memory);
const start = Date.now();
let cgrid;
let hostname;
let log;
let dlog;

if (!opt.options.slow)
  hostname = opt.options.MyHost || os.hostname();

if (process.env.SKALE_DEBUG > 1) {
  log = function () {
    const args = Array.prototype.slice.call(arguments);
    args.unshift('[worker-controller ' + (Date.now() - start) / 1000 + 's]');
    console.error.apply(null, args);
  };
} else {
  log = function () {};
}

if (cluster.isMaster) {
  process.title = 'skale-worker-controller';
  if (memory)
    cluster.setupMaster({execArgv: ['--expose-gc', '--max_old_space_size=' + memory]});
  cluster.on('exit', handleExit);
  const cpus = os.cpus();
  cgrid = new SkaleClient({
    debug: debug,
    retry: opt.options.retry,
    host: opt.options.Host,
    port: opt.options.Port,
    data: {
      type: 'worker-controller',
      hostname: hostname,
      nworkers: nworkers,
      ncpus: cpus.length,
      memory: os.totalmem(),
      platform: os.platform(),
      arch: os.arch(),
      cpumodel: cpus[0].model,
      cpuspeed: cpus[0].speed
    }
  });
  cgrid.on('connect', startWorkers);
  cgrid.on('getWorker', startWorkers);
  cgrid.on('close', process.exit);
  cgrid.on('sendFile', function (msg) {
    fs.createReadStream(msg.path, msg.opt).pipe(cgrid.createStreamTo(msg));
  });
  // Periodic stats
  fs.mkdir('/tmp/skale', function () {});
  setInterval(function () {
    const stats = { nworkers: Object.keys(cluster.workers).length };
    fs.writeFile('/tmp/skale/worker-controller-stats', JSON.stringify(stats), function () {});
  }, 3000);
  log('worker controller ready');
} else {
  runWorker(opt.options.Host, opt.options.Port);
}

function startWorkers(msg) {
  const worker = [];
  const removed = {};
  const n = msg.n || nworkers;

  log('worker-controller host', cgrid.uuid);
  for (let i = 0; i < n; i++) {
    worker[i] = cluster.fork({wsid: msg.wsid, rank: i, puuid: cgrid.uuid});
  }
  worker.forEach(function (w) {
    w.on('message', function (msg) {
      switch (msg.cmd) {
      case 'rm':
        if (msg.dir && !removed[msg.dir]) {
          removed[msg.dir] = true;
          child_process.execFile('/bin/rm', ['-rf', msg.dir]);
        }
        break;
      default:
        console.error('unexpected msg', msg);
      }
    });
  });
}

function handleExit(worker, code, signal) {
  log('worker pid', worker.process.pid, ', exited:', signal || code);
}

function runWorker(host, port) {
  const start = Date.now();
  let wid = process.env.rank;
  let basedir;
  let log;

  if (process.env.SKALE_DEBUG > 1) {
    log = function () {
      const args = Array.prototype.slice.call(arguments);
      args.unshift('[worker-' +  wid + ' ' + (Date.now() - start) / 1000 + 's]');
      console.error.apply(null, args);
    };
    dlog = function() {
      const args = Array.prototype.slice.call(arguments);
      const now = Date.now();
      const lstart = args.shift();
      args.unshift('[worker-' +  wid + ' ' + (now - start) / 1000 + 's]');
      args.push('in ' + (now - lstart) / 1000 + 's');
      console.error.apply(null, args);
    };
  } else {
    dlog = log = function () {};
  }
  if (process.env.SKALE_RANDOM_SEED)
    Dataset.setRandomSeed(process.SKALE_RANDOM_SEED);
  process.on('uncaughtException', function (err) {
    grid.send(grid.muuid, {cmd: 'workerError', args: err.stack});
    process.exit(2);
  });

  const grid = new SkaleClient({
    debug: debug,
    host: host,
    port: port,
    data: {
      ncpu: os.cpus().length,
      os: os.type(),
      arch: os.arch(),
      usedmem: process.memoryUsage().rss,
      totalmem: os.totalmem(),
      hostname: hostname || process.env.puuid,
      type: 'worker',
      wsid: Number(process.env.wsid),
      jobId: ''
    }
  }, function (err, res) {
    log('id:', res.id, ', uuid:', res.uuid);
    wid = 'w' + res.id;
    grid.host = {uuid: res.uuid, id: res.id};
    process.title = 'skale-worker_w' + res.id;
  });

  grid.on('error', function (err) {
    console.error('grid error', err);
    process.exit(2);
  });

  function runTask(msg) {
    grid.muuid = msg.data.master_uuid;
    const task = parseTask(msg.data.args);
    basedir = task.basedir;
    // set worker side dependencies
    task.workerId = 'w' + grid.id;
    task.mm = mm;
    task.grid = grid;
    // Set dependencies in global scope for user evaluated code in workers
    global.azure = azure;
    global.S3 = S3;
    global.dlog = dlog;
    global.log = log;
    global.Lines = Lines;
    global.mkdirp = mkdirp;
    global.mm = mm;
    global.parquet = parquet;
    global.readSplit = readSplit;
    global.uuid = uuid;

    global.fs = fs;
    global.stream = stream;
    global.url = url;
    global.zlib = zlib;

    // Indirect Eval to set user dependencies bundle in the worker global context
    (0, eval)(task.bundle);
    task.run(function(result) {
      result.workerId = task.workerId;
      grid.reply(msg, null, result);
      if (global.gc && forceGc) {
        setImmediate(function () {
          const gcs = Date.now();
          global.gc();
          dlog(gcs, 'gc');
        });
      }
      else log('no global.gc');
    });
  }

  function runztask(msg) {
    //log('runztask msg', msg);
    const file = msg.data.args;
    grid.muuid = msg.data.master_uuid;

    const s = getReadStreamSync({path: file});
    let data = Buffer.concat([]);

    s.on('data', function (chunk) {
      data = Buffer.concat([data, chunk]);
    });
    s.on('end', function () {
      //log('end stream ztask');
      zlib.gunzip(data, {chunkSize: 65536}, function (err, data) {
        if (err) throw new Error(err);
        msg.data.args = data;
        runTask(msg);
      });
    });

    function getReadStreamSync(fileObj, opt) {
      if (fs.existsSync(fileObj.path)) return fs.createReadStream(fileObj.path, opt);
      if (!fileObj.host) fileObj.host = grid.muuid;
      return grid.createStreamFrom(fileObj.host, {cmd: 'sendFile', path: fileObj.path, opt: opt});
    }
  }

  const request = { runTask: runTask, runztask: runztask };

  grid.on('remoteClose', function () {
    process.send({cmd: 'rm', dir: basedir});
    process.exit();
  });

  grid.on('request', function (msg) {
    try {
      request[msg.data.cmd](msg);
    } catch (error) {
      console.error(error.stack);
      grid.reply(msg, error, null);
    }
  });

  grid.on('sendFile', function (msg) {
    fs.createReadStream(msg.path, msg.opt).pipe(grid.createStreamTo(msg));
  });
}

function MemoryManager(memory = 1024) {
  const Mb = 1024 * 1024;
  const MAX_MEMORY = (memory - 100) * Mb;
  const maxStorageMemory = MAX_MEMORY * 0.4;
  const maxShuffleMemory = MAX_MEMORY * 0.2;
  const maxCollectMemory = MAX_MEMORY * 0.2;

  this.storageMemory = 0;
  this.shuffleMemory = 0;
  this.collectMemory = 0;
  this.sizeOf = sizeOf;

  this.storageFull = function () {return (this.storageMemory > maxStorageMemory);};
  this.shuffleFull = function () {return (this.shuffleMemory > maxShuffleMemory);};
  this.collectFull = function () {return (this.collectMemory > maxCollectMemory);};

  this.partitions = {};
  this.register = function (partition) {
    const key = partition.datasetId + '.' + partition.partitionIndex;
    if (!(key in this.partitions)) this.partitions[key] = partition;
  };

  this.unregister = function (partition) {
    this.partitions[partition.datasetId + '.' + partition.partitionIndex] = undefined;
  };

  this.isAvailable = function (partition) {
    return (this.partitions[partition.datasetId + '.' + partition.partitionIndex] !== undefined);
  };
}

function parseTask(str) {
  const task = JSON.parse(str, function (key, value) {
    if (typeof value === 'string') {
      // String value can be a regular function or an ES6 arrow function
      if (value.substring(0, 8) === 'function') {
        const args = value.match(/\(([^)]*)/)[1];
        const body = value.replace(/^function\s*[^)]*\)\s*{/, '').replace(/}$/, '');
        value = new Function(args, body);
      } else if (value.match(/^\s*\(\s*[^(][^)]*\)\s*=>/) || value.match(/^\s*\w+\s*=>/))
        value = ('indirect', eval)(value);
    }
    return value;
  });

  for (let i in task.nodes) {
    const n = task.nodes[i];
    for (let j in n.dependencies) {
      const ref = n.dependencies[j];
      n.dependencies[j] = task.nodes[ref];
    }
    for (let j in n.partitions) {
      Object.setPrototypeOf(task.nodes[i].partitions[j], Dataset.Partition.prototype);
      task.nodes[i].partitions[j].count = 0;
      task.nodes[i].partitions[j].bsize = 0;
      task.nodes[i].partitions[j].tsize = 0;
      task.nodes[i].partitions[j].skip = false;
    }
    if (n.type) {
      Object.setPrototypeOf(task.nodes[i], Dataset[n.type].prototype);
    }
    if (n.partitioner && n.partitioner.type) {
      Object.setPrototypeOf(n.partitioner, Dataset[n.partitioner.type].prototype);
    }
  }
  Object.setPrototypeOf(task, Task.prototype);
  //log('task:', JSON.stringify(task, null, 2));
  return task;
}
