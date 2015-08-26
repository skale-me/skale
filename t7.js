#!/usr/local/bin/node --harmony

var fs = require('fs');
var trace = require('line-trace');
var data = require('./test/support/data.js');
var ugrid = require('./test/support/local.js');
//var ugrid = require('./');

var v = [[1, 1], [1, 1], [2, 3], [2, 4], [3, 5]];

function textParser(s) {return s.split(' ').map(parseFloat);}

var uc = ugrid.context(function () {
	//var a = uc.lineStream(process.stdin, {N: 2});
	var a = uc.lineStream(fs.createReadStream('./test/support/kv.data', {encoding: 'utf8'}), {N: 5}).map(textParser);
	//var a = uc.parallelize(v);

	//a = a.map(function (e) {return e + 2; }); 
	//a = a.union(b);
	//b = b.union(a);

	//var out = a.collect({stream: true});

	//b = b.map(textParser);
	//a = a.coGroup(b);
	a = a.distinct();
	a.reduce(data.reducer, [0, 0], function (err, res) {
		console.log(res);
		//res = res.map(textParser);
		//console.log(res);
	});


	//out.on('data', trace);

	//var out = a.collect({stream:true});
	//out.pipe(process.stdout);
});
