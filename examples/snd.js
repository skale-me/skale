#!/usr/local/bin/node

var UgridClient = require('../lib/ugrid-client.js');
var ugrid = new UgridClient({data: {type: 'snd'}});

ugrid.connect(function(err, res) {
	console.log("uuid: " + res.uuid);
	ugrid.devices({type: 'rcv'}, function(err, res) {
		setInterval(function() {
			for (var i = 0; i < 1000; i++)
				ugrid.request(res[0]);	// Send empty requests
		}, 1);
	});
});
