#!/usr/local/bin/node --harmony

var ugrid = require('ugrid');

var uc = ugrid.context(function () {
	console.log("Connected");
	var a = uc.parallelize([1]);
	a.map(function (grid, a) {console.log(globals); console.log(a)}).collect( function(err,res) {});
});
