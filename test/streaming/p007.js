#!/usr/local/bin/node --harmony

// stream -> collect

var fs = require('fs');
var co = require('co');
var ugrid = require('../..');
var trace = require('line-trace');

var s1 = fs.createReadStream(__dirname + '/f', {encoding: 'utf8'});

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var out = uc.stream(s1, {N: 4}).collect(ondata);

 	function ondata(err, res) {
		if (err == null && res == null) {
			return uc.end();
		}
		console.log(res);
		console.assert(res.length == 4);
	}
}).catch(ugrid.onError);
