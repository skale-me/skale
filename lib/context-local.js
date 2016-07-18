// Copyright 2016 Luca-SAS, licensed under the Apache License 2.0

'use strict';

var child_process = require('child_process');
var fs = require('fs');
var os = require('os');
var zlib = require('zlib');
var mkdirp = require('mkdirp');
var rimraf = require('rimraf');
var uuid = require('node-uuid');
var dataset = require('./dataset.js');
var Task = require('./task.js');

var start = process.hrtime();
var memory = Number(process.env.SKALE_MEMORY) || 4096;

module.exports = Context;

function Context(args) {
  if (!(this instanceof Context))
    return new Context(args);
  this.contextId = uuid.v4();
  var nworker = Number(process.env.SKALE_WORKERS) || (os.cpus().length - 1) || 1;
  var tmp = process.env.SKALE_TMP || '/tmp';
  var self = this;
  this.worker = new Array(nworker);
  for (var i = 0; i < nworker; i++) {
    this.worker[i] = new Worker(i);
  }
  this.jobId = 0;

  process.title = 'skale-master';

  this.basedir = tmp + '/skale/' + this.contextId + '/';
  mkdirp.sync(this.basedir + 'tmp');
  mkdirp.sync(this.basedir + 'stream');

  this.datasetIdCounter = 0;  // global dataset id counter

  this.parallelize = function (localArray, nPartitions) { return dataset.parallelize(this, localArray, nPartitions);};
  this.range = function (start, end, step, nPartitions) { return dataset.range(this, start, end, step, nPartitions);};
  this.lineStream = function (stream, config) {return new dataset.Stream(this, stream, 'line', config);};
  this.objectStream = function (stream, config) {return new dataset.Stream(this, stream, 'object', config);};
  this.log = log;

  this.textFile = function (file, nPartitions) {
    if (file.slice(-1) === '/') return new dataset.TextDir(this, file);
    if (file.slice(-3) === '.gz') return new dataset.GzipFile(this, file);
    return new dataset.TextFile(this, file, nPartitions);
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
      });
    }

    var wid = getLeastBusyWorkerId(task.nodes[task.datasetId].getPreferedLocation(task.pid));
    this.worker[wid].ntask++;
    var str = serialize(task);
    log('task size for worker ' + wid + ':', str.length);
    if (str.length > 1000000) {
      zlib.gzip(str, function (err, res) {
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
    //var stream = new this.createReadStream(jobId, opt); // user readable stream instance

    //this.getWorkers(function () {
    findShuffleStages(function(shuffleStages) {
      if (shuffleStages.length == 0) runResultStage();
      else {
        var cnt = 0;
        runShuffleStage(shuffleStages[cnt], shuffleDone);
      }
      function shuffleDone() {
        if (++cnt < shuffleStages.length) {
          runShuffleStage(shuffleStages[cnt], shuffleDone);
        } else runResultStage();
      }
    });
    //});

    function runShuffleStage(stage, done) {
      findNodes(stage, function(nodes) {
        var pid = 0, shuffleTasks = [], i, j;
        stage.shufflePartitions = [];

        for (i = 0; i < stage.dependencies.length; i++) {
          for (j = 0; j < stage.dependencies[i].partitions.length; j++)
            stage.shufflePartitions[pid++] = new dataset.Partition(stage.id, pid, stage.dependencies[i].id, stage.dependencies[i].partitions[j].partitionIndex);
        }

        for (i = 0; i < stage.shufflePartitions.length; i++)
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
              if (ntasks > nworker) ntasks = nworker;
              if (ntasks > 0) return runBatch(ntasks, index, done);
              stage.executed = true;
              done();
            });
          }
        }

        var n = shuffleTasks.length < nworker ? shuffleTasks.length : nworker;
        log('start shuffle stage, partitions:', stage.shufflePartitions.length);
        runBatch(n, 0, done);
      });
    }

    function runResultStage() {
      findNodes(root, function(nodes) {
        var tasks = [];
        log('start result stage, partitions:', root.partitions.length);
        for (var i = 0; i < root.partitions.length; i++) {
          tasks.push(new Task(self.basedir, jobId, nodes, root.id, i, action));
        }
        //callback({id: jobId, stream: stream}, tasks);
        callback({id: jobId}, tasks);
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
  };

  function rpc(cmd, workerNum, args, callback) {
    //self.request(self.worker[workerNum], {cmd: cmd, args: args, master_uuid: self.uuid, worker: self.worker}, callback);
    self.worker[workerNum].request({cmd: cmd, args: args}, callback);
  }
}

Context.prototype.log = log;

Context.prototype.end = function () {
  rimraf(this.basedir, function (err) {
    if (err) log('remove', err);
  });
  for (var i = 0; i < this.worker.length; i++) {
    this.worker[i].child.disconnect();
  }
};

function Worker(index) {
  this.index = index;
  this.reqid = 1;
  this.reqcb = {};
  this.ntask = 0;
  this.child = child_process.fork(__dirname + '/worker-local.js', [index, memory], {
    execArgv: ['--max_old_space_size=' + memory]
  });
  var self = this;

  this.child.on('message', function(msg) {
    if (self.reqcb[msg.id]) {
      self.reqcb[msg.id](msg.error, msg.result);
      delete self.reqcb[msg.id];
    }
  });
}

Worker.prototype.send = function(msg) {
  this.child.send(msg);
};

Worker.prototype.request = function (req, done) {
  this.reqcb[this.reqid] = done;
  this.child.send({id: this.reqid++, req: req});
};

if (process.env.SKALE_DEBUG) {
  var log =  function() {
    var args = Array.prototype.slice.call(arguments);
    var elapsed = process.hrtime(start);
    args.unshift('[master ' + (elapsed[0] + elapsed[1] / 1e9).toFixed(3) + 's]');
    console.log.apply(null, args);
  };
} else {
  log = function () {};
}
