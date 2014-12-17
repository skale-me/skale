#!/usr/local/bin/node --harmony

var readline = require('readline');
var thunkify = require('thunkify');
var co = require('co');
var grid = require('../lib/ugrid-client.js')({data: {type: 'ping'}});

var rl = readline.createInterface({input: process.stdin, output: process.stdout});

var ask_cb = function (str, callback) {
	process.stdout.write(str);
	rl.once('line', function (res) {
		callback(null, res);
	});
}

var ask = thunkify(ask_cb);

co(function *() {
	yield grid.connect();
	var pong = yield grid.devices({type: 'pong'});
	console.log(pong[0]);
	while (true) {
		var line = yield ask('ping> ');
		grid.send_cb({cmd: 'ping', data: line, id: pong[0].id});
	}
})();
