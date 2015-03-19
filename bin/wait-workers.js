#!/usr/local/bin/node

var grid = require('../lib/ugrid-client.js')({data: {type: 'ww'}});

var nworkers = process.argv[2] || 1;

function waitWorkers() {
	grid.devices({type: 'worker'}, function (err, res) {
		if (err) process.exit(1);
		if (res.length >= nworkers) {
			//grid.send_cb(0, {cmd: 'get', data: res[0].uuid}, function (err, res) {
			//	console.log(res);
			//	process.exit(0);
			//});
			process.exit(0);
		}
		setTimeout(waitWorkers, 1000);
	});
}

waitWorkers();
