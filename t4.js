#!/usr/local/bin/node --harmony

var fs = require('fs');
var trace = require('line-trace');
var ugrid = require('./test/support/local.js');
//var ugrid = require('./');

var uc = ugrid.context(function () {
	var a = uc.lstream(process.stdin, {N: 2});

	a = a.map(function (e) {return e + 2; }); 

	var out = a.col2();

	out.on('data', trace);

	//var out = a.collect({stream:true});
	//out.pipe(process.stdout);
});
