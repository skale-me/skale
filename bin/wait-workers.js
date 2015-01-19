#!/usr/local/bin/node

var grid = require('../lib/ugrid-client.js')({data: {type: 'ww'}});

var nworkers = process.argv[2];

grid.connect_cb(function () {
	function waitWorkers() {
		grid.devices_cb({type: 'worker'}, function (err, res) {
			if (err) process.exit(1);
			if (res.length >= nworkers) process.exit(0);
			setTimeout(waitWorkers, 1000);
		});
	}
	waitWorkers();
});
