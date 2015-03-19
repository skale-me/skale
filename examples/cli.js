#!/usr/local/bin/node

'use strict';

var Client = require('../lib/ugrid-client.js');
var client = new Client({data: {type: 'truc'}});
var input = process.stdin;
if (process.argv[2]) input = require('fs').createReadStream(process.argv[2]);

client.devices({type: 'truc'}, function (err, res) {
	//var pub = new PubStream({client: client, dest: res[0], cmd: 'line'});
	var pub = client.createWriteStream(res[0], 'line');
	input.pipe(pub);
});

client.pipe('line', process.stdout);

//client.on('line', function (msg, done) {
//	console.log(msg); done();
//});
