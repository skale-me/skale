// Copyright 2016 Luca-SAS, licensed under the Apache License 2.0

'use strict';

var stream = require('stream');
var util = require('util');

var Lines = module.exports = function Lines(opt) {
  if (!(this instanceof Lines)) return new Lines(opt);
  stream.Transform.call(this, {objectMode: true});
  this._buf = '';
};
util.inherits(Lines, stream.Transform);

Lines.prototype._transform = function (chunk, encoding, done) {
  var data = this._buf + chunk.toString();
  var lines = data.split('\n');
  this._buf = lines.pop();
  done(null, lines);
};

Lines.prototype._flush = function (done) {
  if (this._buf) this.push([this._buf]);
  done();
};
