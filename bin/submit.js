#!/usr/bin/env node

var fs = require('fs');
var http = require('http');
var https = require('https');
var url = require('url');

if (process.argv.length < 3) {
	console.log('Usage: submit program_file [args...]');
	process.exit(1);
}

var proto = {"http:": http, "https:": https};
var href = url.parse(process.env.UGRID_HOST || 'http://localhost:8000');
var src = process.argv[2];
var args = process.argv.slice(3);

submit(src, args);

function submit(src, args) {
	fs.readFile(src, {encoding: 'utf8'}, function (err, data) {
		if (err) throw err;

		var postdata = JSON.stringify({src: data, args: args});

		var options = {
			hostname: href.hostname,
			port: href.port,
			path: '/run',
			method: 'POST',
			headers: {
				'Content-Type': 'application/json',
				'Content-Length': Buffer.byteLength(postdata)
			}
		};
		if (process.env.UGRID_ACCESS)
			options.headers['X-Auth'] = process.env.UGRID_ACCESS;

		var req = proto[href.protocol].request(options, function (res) {
			res.setEncoding('utf8');
			res.pipe(process.stdout);
		});

		req.on('error', function (err) {throw err;});
		req.end(postdata);
	});
}
