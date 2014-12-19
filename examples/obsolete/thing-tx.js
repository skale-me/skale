#!/usr/local/bin/node --harmony

var UgridClient = require('../lib/ugrid-client.js');

var grid = new UgridClient({
	host: 'localhost',
	port: 12346,
	data: {type: 'publisher'}
});

grid.connect_cb(function(err, res) {
	// Say hello every second
	setInterval(function(){
		grid.send_cb('publish', {uuid: "*", payload: {hello: 'world'}}, function(err, res) {
			if (err) console.log(err);
		});
	}, 1000)
});
