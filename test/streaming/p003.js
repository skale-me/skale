#!/usr/local/bin/node --harmony

// stream1 -> collect
// stream2 -> collect

var fs = require('fs');
var co = require('co');
var ugrid = require('../..');

var s1 = fs.createReadStream('./f', {encoding: 'utf8'});
var s2 = fs.createReadStream('./f2', {encoding: 'utf8'});

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var dist1 = [], dist2 = [], cnt = 0;

	var out1 = uc.stream(s1, {N: 4}).collect({stream: true});
	var out2 = uc.stream(s2, {N: 4}).collect({stream: true});

	out1.on('data', function(res) {dist1.push(res);});
	out2.on('data', function(res) {dist2.push(res);});	

	out1.on('end', function() {if (++cnt == 2) end();})
	out2.on('end', function() {if (++cnt == 2) end();})	

	function end() {
		console.log(dist1);
		console.log(dist2);		
		console.assert(dist1.length == 1);
		console.assert(dist2.length == 1);
		console.assert(dist1[0].length == 4);
		console.assert(dist2[0].length == 4);
		uc.end();
	}


}).catch(ugrid.onError);
