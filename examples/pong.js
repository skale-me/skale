#!/usr/local/bin/node --harmony

var grid = require('../lib/ugrid-client.js')({data: {type: 'pong'}});

grid.on('request', function (msg) {
	console.log(msg);
	console.log('answser in 5s');
	setTimeout(function () {
		grid.reply(msg, null, "xxx " + msg.data);
		console.log('answered');
	}, 5000);
});
