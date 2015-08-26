#!/usr/local/bin/node --harmony

var fs = require('fs');
var trace = require('line-trace');
//var ugrid = require('./test/support/local.js');
var ugrid = require('./');

var uc = ugrid.context(function () {
	var s = fs.createReadStream('/tmp/marc/kv.data', {encoding: 'utf8'});

	var a = uc.lineStream(s, {N: 5});

	var out = a.collect({stream:true});
	out.pipe(process.stdout);
});
