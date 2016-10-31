// Copyright 2016 Luca-SAS, licensed under the Apache License 2.0

'use strict';

var fs = require('fs');
var util = require('util');
var url = require('url');
var zlib = require('zlib');
var uuid = require('node-uuid');
var mkdirp = require('mkdirp');

var SkaleClient = require('./client.js');
var dataset = require('./dataset.js');
var Task = require('./task.js');

module.exports = SkaleContext;

var start = process.hrtime();

util.inherits(SkaleContext, SkaleClient);

function SkaleContext(arg) {
  if (!(this instanceof SkaleContext))
    return new SkaleContext(arg);
  var self = this;

  arg = arg || {};
  arg.data = arg.data || {};
  arg.data.type = 'master';
  SkaleClient.call(this, arg);
  var maxWorker = process.env.SKALE_MAX_WORKER;
  var tmp = arg.tmp || process.env.SKALE_TMP || '/tmp';

  this.started = this.ended = false;
  this.jobId = 0;
  this.worker = [];
  this.log = log;
  this.contextId = uuid.v4(); // context id which will be used as scratch directory name

  this.basedir = tmp + '/skale/' + this.contextId + '/';
  mkdirp.sync(this.basedir + 'tmp');
  mkdirp.sync(this.basedir + 'stream');

  this.once('connect', function(data) {
    var i;
    process.title = 'skale-master_' + data.devices[0].wsid + ' ' + __filename;
    if (!maxWorker || maxWorker > data.devices.length)
      maxWorker = data.devices.length;
    for (i = 0; i < maxWorker; i++) {
      self.worker.push(new Worker(data.devices[i]));
    }
    self.started = true;
  });

  this.on('workerError', function workerError(msg) {
    console.error('Error from worker id %d:', msg.from);
    console.error(msg.args);
  });

  this.on('remoteClose', function getWorkerClose() {
    throw 'Fatal error: unexpected worker exit';
  });

  this.getWorkers = function (callback) {
    if (self.started) return callback();
    this.once('connect', callback);
  };

  function Worker(w) {
    this.uuid = w.uuid;
    this.id = w.id;
    this.ip = w.ip;
    this.ntask = 0;
  }

  Worker.prototype.rpc = function (cmd, args, done) {
    self.request({uuid: this.uuid, id: this.id}, {cmd: cmd, args: args}, done);
  };

  Worker.prototype.send = function (cmd, args) {
    self.send(this.uuid, {cmd: cmd, args: args});
  };

  this.on('request', function (msg) { // Protocol to handle stream flow control: reply when data is consumed
    if (msg.data.cmd == 'stream') {
      self.emit(msg.data.stream, msg.data.data, function() {
        try {self.reply(msg);} catch(err) { console.log(err); }
      });
    }
  });

  this.on('sendFile', function (msg) {
    fs.createReadStream(msg.path, msg.opt).pipe(self.createStreamTo(msg));
  });

  this.end = function () {
    if (self.ended) return;
    self.ended = true;
    if (this.started) self.set({complete: 1});
    self._end();
  };

  this.datasetIdCounter = 0;  // global dataset id counter

  this.parallelize = function (localArray, nPartitions) { return dataset.parallelize(this, localArray, nPartitions);};
  this.range = function (start, end, step, nPartitions) { return dataset.range(this, start, end, step, nPartitions);};
  this.lineStream = function (stream, config) {return new dataset.Stream(this, stream, 'line', config);};
  this.objectStream = function (stream, config) {return new dataset.Stream(this, stream, 'object', config);};

  this.textFile = function (file, nPartitions) {
    var u = url.parse(file);

    if (u.protocol === 's3:') {
      if (file.slice(-1) === '/')
        return new dataset.TextS3Dir(this, file.slice(5, -1));
      return new dataset.TextS3File(this, file.slice(5));
    }
    if (file.slice(-1) === '/')
      return new dataset.TextDir(this, file);
    if (file.slice(-3) === '.gz')
      return new dataset.GzipFile(this, file);
    return new dataset.TextFile(this, file, nPartitions);
  };

  this.getReadStream = function (fileObj, opt) {
    if (fs.existsSync(fileObj.path))
      return fs.createReadStream(fileObj.path, opt);
    return this.createStreamFrom(fileObj.host, {cmd: 'sendFile', path: fileObj.path, opt: opt});
  };

  this.runTask = function(task, callback) {
    function getLeastBusyWorkerId(/* preferredLocation */) {
      var wid, ntask;
      for (var i = 0; i < self.worker.length; i++) {
        if ((ntask == undefined) || (ntask > self.worker[i].ntask)) {
          ntask = self.worker[i].ntask;
          wid = i;
        }
      }
      return wid;
    }

    function serialize(obj) {
      return str = JSON.stringify(obj,function(key, value) {
        if (key == 'sc') return undefined;
        if (key == 'dependencies') {
          var dep = [];
          for (var i = 0; i < value.length; i++) dep[i] = value[i].id;
          return dep;
        }
        return (typeof value === 'function' ) ? value.toString() : value;
      }, 2);
    }

    var wid = getLeastBusyWorkerId(task.nodes[task.datasetId].getPreferedLocation(task.pid));
    this.worker[wid].ntask++;
    var str = serialize(task);
    log('task size for worker ' + wid + ':', str.length);
    log('task', str);
    if (str.length > 1000000) {
      zlib.gzip(str, {chunkSize: 65536}, function (err, res) {
        var filename = task.basedir + 'task-' + uuid.v4() + '.gz';
        fs.writeFile(filename, res, function (err) {
          if (err) throw new Error(err);
          rpc('runztask', wid, filename, function (err, res) {
            self.worker[wid].ntask--;
            callback(err, res);
          });
        });
      });
    } else {
      rpc('runTask', wid, str, function(err, res) {
        self.worker[wid].ntask--;
        callback(err, res);
      });
    }
  };

  this.runJob = function(opt, root, action, callback) {
    var jobId = this.jobId++;
    var stream = new this.createReadStream(jobId, opt); // user readable stream instance

    this.getWorkers(function () {
      findShuffleStages(function(shuffleStages) {
        if (shuffleStages.length == 0) runResultStage();
        else {
          var cnt = 0;
          runShuffleStage(shuffleStages[cnt], shuffleDone);
        }
        function shuffleDone() {
          if (++cnt < shuffleStages.length) runShuffleStage(shuffleStages[cnt], shuffleDone);
          else runResultStage();
        }
      });
    });

    function runShuffleStage(stage, done) {
      findNodes(stage, function(nodes) {
        var pid = 0, shuffleTasks = [], i, j;
        stage.shufflePartitions = {};

        for (i = 0; i < stage.dependencies.length; i++) {
          for (j = 0; j < stage.dependencies[i].nPartitions; j++)
            stage.shufflePartitions[pid++] = new dataset.Partition(stage.id, pid, stage.dependencies[i].id, stage.dependencies[i].partitions[j].partitionIndex);
        }
        stage.nShufflePartitions = pid;

        for (i = 0; i < stage.nShufflePartitions; i++)
          shuffleTasks.push(new Task(self.basedir, jobId, nodes, stage.id, i));

        function runBatch(ntasks, index, done) {
          var cnt = 0, tmax = index + ntasks - 1;
          log('start shuffle tasks', index + '-' + tmax);
          for (var i = 0; i < ntasks; i ++) {
            self.runTask(shuffleTasks[index + i], function (err, res) {
              stage.shufflePartitions[res.pid].files = res.files;
              if (++cnt < ntasks) return;
              index += ntasks;
              ntasks = shuffleTasks.length - index;
              log('finished shuffle tasks:', index + '/' + shuffleTasks.length);
              if (ntasks > maxWorker) ntasks = maxWorker;
              if (ntasks > 0) return runBatch(ntasks, index, done);
              stage.executed = true;
              done();
            });
          }
        }

        var n = shuffleTasks.length < maxWorker ? shuffleTasks.length : maxWorker;
        log('start shuffle stage, partitions:', stage.nShufflePartitions);
        runBatch(n, 0, done);
      });
    }


    function runResultStage() {
      findNodes(root, function(nodes) {
        var tasks = [];
        for (var i = 0; i < root.nPartitions; i++)
          tasks.push(new Task(self.basedir, jobId, nodes, root.id, i, action));
        callback({id: jobId, stream: stream}, tasks);
      });
    }

    function findNodes(node, done) {
      var nodes = {};
      interruptibleTreewalk(node, function cin(n, done) {
        done(n.shuffling && n.executed);
      }, function cout(n, done) {
        n.getPartitions(function() {
          if (nodes[n.id] == undefined) nodes[n.id] = n;
          done();
        });
      }, function() {done(nodes);});
    }

    function findShuffleStages(callback) {
      var stages = [];
      interruptibleTreewalk(root, function cin(n, done) {
        if (n.shuffling && !n.executed) stages.unshift(n);
        done(n.shuffling && n.executed);  // stage boundary are shuffle nodes
      }, function cout(n, done) {done();}, function() {callback(stages);});
    }

    function interruptibleTreewalk(n, cin, cout, done) {
      cin(n, function(uturn) { // if uturn equals true the subtree under node won't be treewalked
        if (!uturn) {
          var nDependencies = 0;
          for (var i = 0; i < n.dependencies.length; i++)
            interruptibleTreewalk(n.dependencies[i], cin, cout, function() {
              if (++nDependencies == n.dependencies.length) cout(n, done);
            });
          if (n.dependencies.length == 0) cout(n, done);
        } else cout(n, done);
      });
    }

    return stream;
  };

  function rpc(cmd, workerNum, args, callback) {
    self.request(self.worker[workerNum], {cmd: cmd, args: args, master_uuid: self.uuid, worker: self.worker}, callback);
  }
}

if (process.env.SKALE_DEBUG) {
  var log =  function() {
    var args = Array.prototype.slice.call(arguments);
    var elapsed = process.hrtime(start);
    args.unshift('[master ' + (elapsed[0] + elapsed[1] / 1e9).toFixed(3) + 's]');
    console.error.apply(null, args);
  };
} else {
  log = function () {};
}
