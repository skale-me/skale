#!/usr/local/bin/node --harmony

var ugrid = require('../..');

var uc = ugrid.context(function () {
	console.log("Connected");
	process.exit(0);
});
