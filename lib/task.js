'use strict';

var http = require('http');

var uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-4][0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

module.exports = Task;

// function Task(basedir, jobId, nodes, datasetId, pid, action) {
function Task(init) {
  this.basedir = init.basedir;
  this.datasetId = init.datasetId;
  this.pid = init.pid;
  this.nodes = init.nodes;
  this.action = init.action;
  this.files = {};      // object in which we store shuffle file informations to be sent back to master
//  this.lib;         // handler to libraries required on worker side (which cannot be serialized)
//  this.mm;          // handler to worker side memory manager instance 
//  this.grid;          // handler to socket object instance
}

Task.prototype.run = function(done) {
  var pipeline = [], self = this, mm = this.mm, action = this.action;
  var p = this.pid;
  var tmpPart = action ? this.nodes[this.datasetId].partitions[p] : this.nodes[this.datasetId].shufflePartitions[p];
  var tmpDataset = this.nodes[tmpPart.datasetId];
  var blocksToRegister = [];

  this.lib.mkdirp.sync(this.basedir + 'export');
  this.lib.mkdirp.sync(this.basedir + 'import');
  this.lib.mkdirp.sync(this.basedir + 'shuffle');

  // Propagate environment settings from master
  if (this.env) {
    this.log('env:', this.env);
    for (var e in this.env) {
      if (this.env[e]) process.env[e] = this.env[e];
      else delete process.env[e];
    }
  }

  if (action) {
    if (action.opt._foreach) {
      pipeline.push({transform: function foreach(context, data) {
        for (var i = 0; i < data.length; i++) action.src(data[i], action.opt, self);
      }});
    } else {
      pipeline.push({transform: function aggregate(context, data) {
        for (var i = 0; i < data.length; i++)
          action.init = action.src(action.init, data[i], action.opt, self);
      }});
    }
  }

  for (;;) {
    var tmpPartAvailable = mm.isAvailable(tmpPart);             // is partition available in memory
    if (!tmpPartAvailable && tmpDataset.persistent) {             // if data must be stored in memory
      if ((action != undefined) || (tmpDataset.id != this.datasetId)) {       // no persist if no action and shuffleRDD
        blocksToRegister.push(tmpPart);                 // register block inside memory manager
        pipeline.unshift(tmpPart);                    // add it to pipeline
        tmpPart.mm = this.mm;
      }
    }
    if (tmpPartAvailable || (tmpPart.parentDatasetId == undefined)) break;    // source partition found
    pipeline.unshift(tmpDataset);                       // else add current dataset transform to pipeline
    tmpPart = this.nodes[tmpPart.parentDatasetId].partitions[tmpPart.parentPartitionIndex];
    tmpDataset = this.nodes[tmpPart.datasetId];
  }

  // Pre-iterate actions
  if (action) {
    if (action.opt._preIterate) {
      action.opt._preIterate(action.opt, this, tmpPart.partitionIndex);
    }
  }

  // Iterate actions
  if (tmpPartAvailable) mm.partitions[tmpPart.datasetId + '.' + tmpPart.partitionIndex].iterate(this, tmpPart.partitionIndex, pipeline, iterateDone);
  else this.nodes[tmpPart.datasetId].iterate(this, tmpPart.partitionIndex, pipeline, iterateDone);

  // Post-iterate actions
  function iterateDone() {
    blocksToRegister.map(function(block) {mm.register(block);});
    if (action) {
      if (action.opt._postIterate) {
        action.opt._postIterate(action.init, action.opt, self, tmpPart.partitionIndex, function () {
          done({data: {host: self.grid.host.uuid, path: self.exportFile}});
        });
      } else done({data: action.init});
    } else self.nodes[self.datasetId].spillToDisk(self, function() {
      done({pid: self.pid, files: self.files});
    });
  }
};

// Get a readable stream for shuffle or source file.
// First, attempt to read from local filesystem
// If not present, attempt to access an HTTP server
// If HTTP server not available, use skale transport through skale server
Task.prototype.getReadStream = function (fileObj, opt, done) {
  var fs = this.lib.fs;
  if (fs.existsSync(fileObj.path)) return done(null, fs.createReadStream(fileObj.path, opt));
  // Default host is master
  if (!fileObj.host) fileObj.host = this.grid.muuid;
  if (uuidPattern.test(fileObj.host))
    return done(null, this.grid.createStreamFrom(fileObj.host, {cmd: 'sendFile', path: fileObj.path, opt: opt}));
  var url = 'http://' + fileObj.host + fileObj.path;
  http.get(url, function (res) {
    done(null, res);
  });
};

// Same as above getReadStream, but return a streams synchronously.
// This may be more expensive, as it requires an additional pass-through stream
Task.prototype.getReadStreamSync = function (fileObj, opt) {
  var fs = this.lib.fs;
  if (fs.existsSync(fileObj.path)) return fs.createReadStream(fileObj.path, opt);
  if (!fileObj.host) fileObj.host = this.grid.muuid;
  return this.grid.createStreamFrom(fileObj.host, {cmd: 'sendFile', path: fileObj.path, opt: opt});
};
