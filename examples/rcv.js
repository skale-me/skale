#!/usr/local/bin/node

var UgridClient = require('../lib/ugrid-client.js');
var ugrid = new UgridClient({data: {type: 'rcv'}});

ugrid.connect_cb(function(err, res) {
	console.log("uuid: " + res.uuid);
});
