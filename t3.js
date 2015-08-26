#!/usr/local/bin/node --harmony

var fs = require('fs');
var trace = require('line-trace');
var ugrid = require('./test/support/local.js');
//var ugrid = require('./');

var uc = ugrid.context(function () {
	var s = fs.createReadStream('./test/support/kv.data', {encoding: 'utf8'});

	var a = uc.lstream(s, {N: 5});

	trace(a);
	//var out = a.collect({stream:true});
	//out.pipe(process.stdout);
});
