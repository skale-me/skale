#!/usr/local/bin/node --harmony

var UgridClient = require('../lib/ugrid-client.js');

var grid = new UgridClient({
	host: 'localhost',
	port: 12346	
});

grid.connect_cb(function(err, res) {
	console.log('Logged with uuid: ' + res.uuid);

	// Query devices, get uuid array
	grid.send_cb('devices', {type: "publisher"}, function(err, res) {
		if (err) console.log(err);
		if (res.length == 0)
			return;
		var dev_uuid = res.devices[0];
		// Subscribe to first device
		grid.send_cb('subscribe', {uuid: dev_uuid}, function(err, res) {
			if (err) console.log(err);
			else console.log("Subscribed to #" + dev_uuid);
		});
	})

	// Receive messages
	grid.on('message', function(data) {
		console.log(data);
	})
});
