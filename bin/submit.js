#!/usr/bin/env node

var fs = require('fs');
var http = require('http');
var https = require('https');
var url = require('url');

if (process.argv.length < 4) {
	console.log('Usage: submit.js ugrid_server_url program_file [args...]');
	process.exit(1);
}

var proto = {"http:": http, "https:": https};
var href = url.parse(process.argv[2]);
var access = process.env.UGRID_ACCESS;

fs.readFile(process.argv[3], {encoding: 'utf8'}, function (err, data) {
	if (err) throw err;

	var postdata = JSON.stringify({access: access, src: data, args: process.argv.slice(4)});

	var options = {
		hostname: href.hostname,
		port: href.port,
		path: '/run',
		method: 'POST',
		headers: {
			'X-Auth': process.env.UGRID_ACCESS,
			'Content-Type': 'application/json',
			'Content-Length': Buffer.byteLength(postdata)
		}
	};

	var response = '';

	var req = proto[href.protocol].request(options, function (res) {
		res.setEncoding('utf8');
		res.pipe(process.stdout);
	});

	req.on('error', function (err) {throw err;});
	req.end(postdata);
});
