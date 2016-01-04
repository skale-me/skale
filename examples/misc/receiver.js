#!/usr/bin/env node

// Receive data through ugrid

var fs = require('fs');
var stream = require('stream')
var UgridClient = require('../../lib/ugrid-client.js');
var trace = require('line-trace');

var uc = new UgridClient({data: {type: 'receiver'}});

uc.on('connect', function (msg) {
	uc.devices({type: 'sender'}, function (err, res) {
		trace(res)
		var fstream = fs.createWriteStream('/tmp/xxx');
		var gs = uc.createStreamFrom(res[0].data.uuid, {cmd: 'fileRequest'});
		gs.pipe(process.stdout)
	})
})
