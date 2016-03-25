// Copyright 2016 Luca-SAS, licensed under the Apache License 2.0

'use strict';

var LocalArray = require('./local-array.js');
var trace = require('line-trace');

module.exports.TextStream = LocalArray.TextStream;
module.exports.context = LocalContext;

function LocalContext(args) {
	if (!(this instanceof LocalContext))
		return new LocalContext(args);
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

LocalContext.prototype.end = function () {};
