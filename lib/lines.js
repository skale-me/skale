'use strict';

var os = require('os');
var stream = require('stream');
var util = require('util');

var Lines = module.exports = function Lines(opts) {
	if (!(this instanceof Lines)) return new Lines(opts);
	opts = opts || {};
	opts.objectMode = true;
	stream.Transform.call(this, opts);
	this._sep = opts.separator || os.EOL;
	this._buf = '';
};
util.inherits(Lines, stream.Transform);

Lines.prototype._transform = function (chunk, encoding, done) {
	var data = this._buf + chunk.toString('utf8');
	var lines = data.split(this._sep);
	this._buf = lines.pop();
	var self = this;
	for (var i in lines)
		this.push(lines[i]);
	done();
};

Lines.prototype._flush = function (done) {
	if (this._buf)
		this.push(this._buf);
	done();
};
