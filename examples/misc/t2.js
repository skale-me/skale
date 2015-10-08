#!/usr/local/bin/node --harmony

var ugrid = require('../..');

var uc = ugrid.context(function () {
	console.log("Connected");
	console.error("sample error message");
	process.exit(0);
});
