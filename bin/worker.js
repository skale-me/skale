#!/usr/bin/env node

// Copyright 2016 Luca-SAS, licensed under the Apache License 2.0

'use strict';

var child_process = require('child_process');
var fs = require('fs');
var os = require('os');
var cluster = require('cluster');
var url = require('url');
var zlib = require('zlib');

var mkdirp = require('mkdirp');
var uuid = require('uuid');
var AWS = require('aws-sdk');
var azure = require('azure-storage');

var SkaleClient = require('../lib/client.js');
var Dataset = require('../lib/dataset.js');
var Task = require('../lib/task.js');
var Lines = require('../lib/lines.js');
var sizeOf = require('../lib/rough-sizeof.js');
var readSplit = require('../lib/readsplit.js').readSplit;
var parquet = require('../lib/parquet.js');

//var global = {require: require};

var opt = require('node-getopt').create([
  ['h', 'help', 'print this help text'],
  ['d', 'debug', 'print debug traces'],
  ['m', 'memory=ARG', 'set max memory in MB for workers'],
  ['M', 'MyHost=ARG', 'advertised hostname (peer-to-peer)'],
  ['n', 'nworker=ARG', 'number of workers (default: number of cpus)'],
  ['s', 'slow', 'disable peer-to-peer file transfers though HTTP'],
  ['H', 'Host=ARG', 'server hostname (default localhost)'],
  ['P', 'Port=ARG', 'server port (default 12346)'],
  ['V', 'version', 'print version']
]).bindHelp().parseSystem();

if (opt.options.version) {
  var pkg = require('../package');
  return console.log(pkg.name + '-' +  pkg.version);
}

var debug = opt.options.debug || false;
var ncpu = Number(opt.options.nworker) || (process.env.SKALE_WORKER_PER_HOST ? process.env.SKALE_WORKER_PER_HOST : os.cpus().length);
var memory = Number(opt.options.memory || process.env.SKALE_MEMORY);
var cgrid;
var mm = new MemoryManager(memory);
var log;
var hostname;
var start = process.hrtime();

ncpu = Number(ncpu);
if (!opt.options.slow)
  hostname = opt.options.MyHost || os.hostname();

if (process.env.SKALE_DEBUG > 1) {
  log = function () {
    var args = Array.prototype.slice.call(arguments);
    var elapsed = process.hrtime(start);
    args.unshift('[worker-controller ' + (elapsed[0] + elapsed[1] / 1e9).toFixed(3) + 's]');
    console.error.apply(null, args);
  };
} else {
  log = function () {};
}

if (cluster.isMaster) {
  process.title = 'skale-worker-controller';
  if (memory)
    cluster.setupMaster({execArgv: ['--max_old_space_size=' + memory]});
  cluster.on('exit', handleExit);
  cgrid = new SkaleClient({
    debug: debug,
    host: opt.options.Host,
    port: opt.options.Port,
    data: {
      type: 'worker-controller',
      hostname: hostname,
      ncpu: ncpu
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
    var stats = { nworkers: Object.keys(cluster.workers).length };
    fs.writeFile('/tmp/skale/worker-controller-stats', JSON.stringify(stats), function () {}); }, 3000);
  log('worker controller ready');
} else {
  runWorker(opt.options.Host, opt.options.Port);
}

function startWorkers(msg) {
  var worker = [], removed = {};
  var n = msg.n || ncpu;
  log('worker-controller host', cgrid.uuid);
  for (var i = 0; i < n; i++)
    worker[i] = cluster.fork({wsid: msg.wsid, rank: i, puuid: cgrid.uuid});
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
  var basedir, log;
  var start = process.hrtime();
  var wid = process.env.wsid + '-' + process.env.rank;
  if (process.env.SKALE_DEBUG > 1) {
    log = function () {
      var args = Array.prototype.slice.call(arguments);
      var elapsed = process.hrtime(start);
      args.unshift('[worker-' +  process.env.rank + ' ' + (elapsed[0] + elapsed[1] / 1e9).toFixed(3) + 's]');
      console.error.apply(null, args);
    };
  } else {
    log = function () {};
  }
  process.title = 'skale-worker_' + wid;
  process.on('uncaughtException', function (err) {
    grid.send(grid.muuid, {cmd: 'workerError', args: err.stack});
    process.exit(2);
  });

  var grid = new SkaleClient({
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
      wsid: process.env.wsid,
      jobId: ''
    }
  }, function (err, res) {
    log('id:', res.id, ', uuid:', res.uuid);
    grid.host = {uuid: res.uuid, id: res.id};
  });

  grid.on('error', function (err) {
    console.error('grid error', err);
    process.exit(2);
  });

  function runTask(msg) {
    grid.muuid = msg.data.master_uuid;
    var task = parseTask(msg.data.args);
    basedir = task.basedir;
    // set worker side dependencies
    task.workerId = grid.host.uuid;
    task.mm = mm;
    task.log = log;
    task.lib = {AWS: AWS, azure: azure, sizeOf: sizeOf, fs: fs, readSplit: readSplit, Lines: Lines, task: task, mkdirp: mkdirp, parquet: parquet, url: url, uuid: uuid, zlib: zlib};
    task.grid = grid;
    task.run(function(result) {
      result.workerId = 'g' + grid.id;
      grid.reply(msg, null, result);
    });
  }

  function runztask(msg) {
    //log('runztask msg', msg);
    var file = msg.data.args;
    grid.muuid = msg.data.master_uuid;

    var s = getReadStreamSync({path: file});
    var data = Buffer.concat([]);
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

  var request = { runTask: runTask, runztask: runztask };

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

function MemoryManager(memory) {
  memory = memory || 1024;
  var Kb = 1024, Mb = 1024 * Kb;
  var MAX_MEMORY = (memory - 100) * Mb;
  var maxStorageMemory = MAX_MEMORY * 0.4;
  var maxShuffleMemory = MAX_MEMORY * 0.2;
  var maxCollectMemory = MAX_MEMORY * 0.2;

  this.storageMemory = 0;
  this.shuffleMemory = 0;
  this.collectMemory = 0;
  this.sizeOf = sizeOf;

  this.storageFull = function () {return (this.storageMemory > maxStorageMemory);};
  this.shuffleFull = function () {return (this.shuffleMemory > maxShuffleMemory);};
  this.collectFull = function () {return (this.collectMemory > maxCollectMemory);};

  this.partitions = {};
  this.register = function (partition) {
    var key = partition.datasetId + '.' + partition.partitionIndex;
    if (!(key in this.partitions)) this.partitions[key] = partition;
  };

  this.unregister = function (partition) {
    this.partitions[partition.datasetId + '.' + partition.partitionIndex] = undefined;
  };

  this.isAvailable = function (partition) {
    return (this.partitions[partition.datasetId + '.' + partition.partitionIndex] != undefined);
  };
}

function parseTask(str) {
  var i, j, n, ref;
  var task = JSON.parse(str, function (key, value) {
    if (typeof value == 'string') {
      // String value can be a regular function or an ES6 arrow function
      if (value.substring(0, 8) == 'function') {
        var args = value.match(/\(([^)]*)/)[1];
        var body = value.replace(/^function\s*[^)]*\)\s*{/, '').replace(/}$/, '');
        value = new Function(args, body);
      } else if (value.match(/^\s*\(\s*[^(][^)]*\)\s*=>/) || value.match(/^\s*\w+\s*=>/))
        value = ('indirect', eval)(value);
    }
    return value;
  });

  for (i in task.nodes) {
    n = task.nodes[i];
    for (j in n.dependencies) {
      ref = n.dependencies[j];
      n.dependencies[j] = task.nodes[ref];
    }
    for (j in n.partitions) {
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
