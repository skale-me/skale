// Copyright 2016 Luca-SAS, licensed under the Apache License 2.0

// worker module

'use strict';

const fs = require('fs');
const zlib = require('zlib');
const url = require('url');
const stream = require('stream');

const mkdirp = require('mkdirp');
const uuid = require('uuid');
const S3 = require('aws-sdk/clients/s3');
const azure = require('azure-storage');
const parquet = require('./stub-parquet.js');

const Dataset = require('./dataset.js');
const Task = require('./task.js');
const sizeOf = require('./rough-sizeof.js');
const Lines = require('./lines.js');
const readSplit = require('./readsplit.js').readSplit;

const workerId = process.argv[2];
let memory = process.argv[3];

const mm = new MemoryManager(memory);

const start = Date.now();

if (process.env.SKALE_RANDOM_SEED)
  Dataset.setRandomSeed(process.env.SKALE_RANDOM_SEED);

process.title = 'skale-worker-' + workerId;

process.on('disconnect', function () {
  log('disconnected, exit');
  process.exit();
});

process.on('message', function (msg) {
  if (typeof msg === 'object' && msg.req) {
    switch (msg.req.cmd) {
    case 'runTask':
      runTask(msg);
      break;
    case 'runztask':
      runztask(msg);
      break;
    }
  }
});

function runztask(msg) {
  const file = msg.req.args;
  fs.readFile(file, function (err, data) {
    fs.unlink(file, function () {});
    if (err) throw new Error(err);
    zlib.gunzip(data, {chunkSize: 65536}, function (err, data) {
      if (err) throw new Error(err);
      msg.req.args = data;
      runTask(msg);
    });
  });
}

function runTask(msg) {
  const task = parseTask(msg.req.args);
  task.workerId = workerId;
  task.grid = {host: {}};
  task.mm = mm;
  //task.log = log;
  //task.dlog = dlog;
  // Expose system core dependencies explicitely for user evaluated code in workers
  // Those dependencies do not need to be serialized
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
  task.run(function (result) {
    delete msg.req.args;
    msg.result = result;
    msg.result.workerId = workerId;
    process.send(msg);
    if (global.gc) {
      setImmediate(function () {
        const gcs = Date.now();
        global.gc();
        dlog(gcs, 'gc');
      });
    }
    else log('no global.gc');
  });
}

function parseTask(str) {
  //var i, j, n, ref;
  const task = JSON.parse(str, function (key, value) {
    if (typeof value === 'string') {
      // String value can be a regular function or an ES6 arrow function
      if (value.substring(0, 8) == 'function') {
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

let log;
let dlog;
if (process.env.SKALE_DEBUG > 1) {
  log = function log() {
    const args = Array.prototype.slice.call(arguments);
    args.unshift('[worker-' +  process.argv[2] + ' ' + (Date.now() - start) / 1000 + 's]');
    console.error.apply(null, args);
  };
  dlog = function dlog() {
    const args = Array.prototype.slice.call(arguments);
    const now = Date.now();
    const lstart = args.shift();
    args.unshift('[worker-' +  process.argv[2] + ' ' + (now - start) / 1000 + 's]');
    args.push('in ' + (now - lstart) / 1000 + 's');
    console.error.apply(null, args);
  };
} else {
  dlog = log = function noop() {};
}
