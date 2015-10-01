#!/usr/local/bin/node --harmony

var ugrid = require('ugrid');

var uc = ugrid.context(function () {
	console.log("Connected");
	process.exit(0);
});
