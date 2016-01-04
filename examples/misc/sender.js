#!/usr/bin/env node

// Send data through ugrid

var fs = require('fs')
var UgridClient = require('../../lib/ugrid-client.js');
var trace = require('line-trace');

var uc = new UgridClient({data: {type: 'sender'}});

uc.on('fileRequest', function fileRequest(msg) {
	trace(msg)
	var gstream = uc.createStreamTo(msg);
	var instream = fs.createReadStream('/etc/hosts');
	instream.pipe(gstream);
})
