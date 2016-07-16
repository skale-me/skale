// Copyright 2016 Luca-SAS, licensed under the Apache License 2.0

'use strict';

var stream = require('stream');
var util = require('util');

var Lines = module.exports = function Lines(opt) {
  if (!(this instanceof Lines)) return new Lines(opt);
  stream.Transform.call(this, {objectMode: true});
  this._buf = '';
  this._newline = false;
};
util.inherits(Lines, stream.Transform);

Lines.prototype._transform = function (chunk, encoding, done) {
  var data = this._buf + chunk.toString('utf8');
  var lines = data.split(/\r\n|\r|\n/);
  this._newline = data[data.length - 1] === '\n';
  this._buf = lines.pop();
  done(null, lines);
};

Lines.prototype._flush = function (done) {
  if (this._buf) this.push([this._buf]);
  done();
  this.emit('endNewline', this._newline);
};
