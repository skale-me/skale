'use strict';

var LocalArray = require('./local-array.js');
var trace = require('line-trace');

module.exports.TextStream = LocalArray.TextStream;
module.exports.context = LocalContext;

function LocalContext(args, done) {
	if (!(this instanceof LocalContext))
		return new LocalContext(args, done);
	if (arguments.length < 3) {
		done = args;
		args = undefined;
	}
	if (done) process.nextTick(done);
}

LocalContext.prototype.lineStream = function (inputStream, opt) {
	var loc = new LocalArray();
	loc.lineStream(inputStream, opt);
	return loc;
};

LocalContext.prototype.parallelize = function (data) {
	var loc = new LocalArray();
	loc.parallelize(data);
	return loc;
};

LocalContext.prototype.textFile = function (path) {
	var loc = new LocalArray();
	loc.textFile(path);
	return loc;
};
