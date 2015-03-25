#!/usr/local/bin/node

var grid = require('../lib/ugrid-client.js')({data: {type: 'wc'}});

function waitController() {
	grid.devices({type: 'controller'}, 0, function (err, res) {
		if (err) process.exit(1);
		if (res.length == 1) {
			//grid.send_cb(0, {cmd: 'get', data: res[0].uuid}, function (err, res) {
			//	console.log(res);
			//	process.exit(0);
			//});
			process.exit(0);
		}
		setTimeout(waitController, 1000);
	});
}

waitController();
