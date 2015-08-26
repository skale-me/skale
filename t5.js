#!/usr/local/bin/node --harmony

var fs = require('fs');
var trace = require('line-trace');
var ugrid = require('./test/support/local.js');
//var ugrid = require('./');

var uc = ugrid.context(function () {
	var a = uc.lineStream(process.stdin, {N: 2});
	//var a = uc.lineStream(fs.createReadStream('./test/support/kv.data', {encoding: 'utf8'}), {N: 2});
	var b = uc.lineStream(fs.createReadStream('./test/support/kv2.data', {encoding: 'utf8'}), {N: 2});

	//a = a.map(function (e) {return e + 2; }); 
	//a = a.union(b);
	b = b.union(a);

	var out = b.collect({stream: true});

	out.on('data', trace);

	//var out = a.collect({stream:true});
	//out.pipe(process.stdout);
});
