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
var uuid = require('node-uuid');
var trace = require('line-trace');
var AWS = require('aws-sdk');

var SkaleClient = require('../lib/client.js');
var Dataset = require('../lib/dataset.js');
var Task = require('../lib/task.js');
var Lines = require('../lib/lines.js');
var sizeOf = require('../lib/rough-sizeof.js');
var readSplit = require('../lib/readsplit.js').readSplit;

//var global = {require: require};

var opt = require('node-getopt').create([
  ['h', 'help', 'print this help text'],
  ['d', 'debug', 'print debug traces'],
  ['m', 'memory=ARG', 'set max memory in MB for workers (default 1024)'],
  ['M', 'MyHost=ARG', 'advertised hostname'],
  ['n', 'nworker=ARG', 'number of workers (default: number of cpus)'],
  ['t', 'tmp=ARG', 'set tmp dirname (default: /tmp)'],
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
var hostname = opt.options.MyHost || os.hostname();
var memory = Number(opt.options.memory || process.env.SKALE_MEMORY || 1024);
var tmp = opt.options.tmp || process.env.SKALE_TMP || '/tmp';
var cgrid;
var mm = new MemoryManager(memory);
ncpu = Number(ncpu);

if (cluster.isMaster) {
  process.title = 'skale-worker-controller';
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
  console.log('worker controller ready');
} else {
  runWorker(opt.options.Host, opt.options.Port);
}

function startWorkers(msg) {
  var worker = [], removed = {};
  var n = msg.n || ncpu;
  console.log('worker-controller host', cgrid.uuid);
  for (var i = 0; i < n; i++)
    worker[i] = cluster.fork({wsid: msg.wsid, rank: i, puuid: cgrid.uuid});
  worker.forEach(function (w) {
    w.on('message', function (msg) {
      switch (msg.cmd) {
      case 'rm':
        if (msg.dir && !removed[msg.dir]) {
          removed[msg.dir] = true;
          trace('remove ' + tmp + '/skale/' + msg.dir);
          child_process.execFile('/bin/rm', ['-rf', tmp + '/skale/' + msg.dir]);
        }
        break;
      default:
        console.log('unexpected msg', msg);
      }
    });
  });
}

function handleExit(worker, code, signal) {
  console.log('worker pid', worker.process.pid, ', exited:', signal || code);
}

function runWorker(host, port) {
  var contextId, log;
  var start = process.hrtime();
  var wid = process.env.wsid + '-' + process.env.rank;
  if (process.env.SKALE_DEBUG > 1) {
    log =  function() {
      var args = Array.prototype.slice.call(arguments);
      var elapsed = process.hrtime(start);
      args.unshift('[worker-' +  wid + ' ' + (elapsed[0] + elapsed[1] / 1e9).toFixed(3) + 's]');
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
      hostname: hostname,
      type: 'worker',
      wsid: process.env.wsid,
      jobId: ''
    }
  }, function (err, res) {
    console.log('id: ', res.id, 'uuid: ', res.uuid);
    grid.host = {uuid: res.uuid, id: res.id};
  });

  grid.on('error', function (err) {
    console.log('grid error', err);
    process.exit(2);
  });

  function runTask(msg) {
    grid.muuid = msg.data.master_uuid;
    var task = parseTask(msg.data.args);
    contextId = task.contextId;
    // set worker side dependencies
    task.workerId = grid.host.uuid;
    task.mm = mm;
    task.log = log;
    task.lib = {AWS: AWS, sizeOf: sizeOf, fs: fs, readSplit: readSplit, Lines: Lines, task: task, mkdirp: mkdirp, url: url, uuid: uuid, trace: trace, zlib: zlib};
    task.grid = grid;
    task.run(function(result) {grid.reply(msg, null, result);});
  }

  function runztask(msg) {
    log('runztask msg', msg);
    var file = msg.data.args;

//    fs.readFile(file, function (err, data) {
//      fs.unlink(file, function () {});
//      if (err) throw new Error(err);
//      zlib.gunzip(data, {chunkSize: 65536}, function (err, data) {
//        if (err) throw new Error(err);
//        msg.data.args = data;
//        runTask(msg);
//      });
//    });

    var s = getReadStream({path: file});
    var data = Buffer.concat([]);
    s.on('data', function (chunk) {
      data = Buffer.concat([data, chunk]);
    });
    s.on('end', function () {
      log('end stream ztask');
      zlib.gunzip(data, {chunkSize: 65536}, function (err, data) {
        if (err) throw new Error(err);
        msg.data.args = data;
        runTask(msg);
      });
    });

    function getReadStream(fileObj, opt) {
      if (fs.existsSync(fileObj.path)) return fs.createReadStream(fileObj.path, opt);
      if (!fileObj.host) fileObj.host = grid.muuid;
      return grid.createStreamFrom(fileObj.host, {cmd: 'sendFile', path: fileObj.path, opt: opt});
    }
  }

  var request = { runTask: runTask, runztask: runztask };

  grid.on('remoteClose', function () {
    process.send({cmd: 'rm', dir: contextId});
    process.exit();
  });

  grid.on('request', function (msg) {
    try {request[msg.data.cmd](msg);} 
    catch (error) {
      console.error(error.stack);
      grid.reply(msg, error, null);
    }
  });

  grid.on('sendFile', function (msg) {
    fs.createReadStream(msg.path, msg.opt).pipe(grid.createStreamTo(msg));
  });
}

function MemoryManager(memory) {
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
