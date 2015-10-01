#!/usr/bin/env node

var fs = require('fs');
var http = require('http');
var url = require('url');

if (process.argv.length < 4) {
	console.log('Usage: submit.js ugrid_server_url program_file [args...]');
	process.exit(1);
}

var href = url.parse(process.argv[2]);

fs.readFile(process.argv[3], {encoding: 'utf8'}, function (err, data) {
	if (err) throw err;

	var postdata = JSON.stringify({src: data, args: process.argv.slice(4)});

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

	var response = '';

	var req = http.request(options, function (res) {
		res.setEncoding('utf8');
		res.on('data', function (d) {response += d;});
		res.on('end', function () {
			var resp = JSON.parse(response);
			if (resp.stdout) process.stdout.write(resp.stdout);
			if (resp.stderr) process.stderr.write(resp.stderr);
			process.exit(resp.err);
		});
	});

	req.on('error', function (err) {throw err;});
	req.end(postdata);
});
