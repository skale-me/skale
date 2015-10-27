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
	var count = lines.length;
	function callback() {if (--count === 0) done();}

	for (var i in lines)
		// this.push(lines[i]);
		this.emit('data', lines[i], callback);
	// done();
};

Lines.prototype._flush = function (done) {
	if (this._buf)
		// this.push(this._buf);	
		this.emit('data', this._buf, done);
	else done();
	this.emit('endNewline', this._newline);
};
