'use strict';

var LocalArray = require('./local-array.js');

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

LocalContext.prototype.lineStream = function (inputStream) {
	var localArray = new LocalArray();
	return localArray.lineStream(inputStream);
};

LocalContext.prototype.parallelize = function (data) {
	var localArray = new LocalArray();
	return localArray.parallelize(data);
};
