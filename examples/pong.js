#!/usr/local/bin/node --harmony

var grid = require('../lib/ugrid-client.js')({host: 'localhost', port: 12346, data: {type: 'pong'}});

grid.connect_cb(function () {
	grid.on('request', function (msg) {
		console.log(msg);
		console.log('answser in 5s');
		setTimeout(function () {
			grid.reply(msg, null, "xxx " + msg.data);
			console.log('answered');
		}, 5000);
	});
});
