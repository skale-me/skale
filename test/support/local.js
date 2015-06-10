'use strict';

var LocalArray = require('./local-array.js');

module.exports.context = LocalContext;

function LocalContext(args) {
	if (!(this instanceof LocalContext))
		return new LocalContext(args);
}

LocalContext.prototype.parallelize = function (data) {
	var localArray = new LocalArray();
	return localArray.parallelize(data);
};
