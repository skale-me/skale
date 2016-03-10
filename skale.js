#!/usr/bin/env node

// Copyright 2016 Luca-SAS, licensed under the Apache License 2.0

const help=`Usage: skale [options] <command> [<args>]

Commands:
  run <file> [<args>...]	Run file on skale cluster

Options:
  -H, --host=<hostname>	Set skale hostname
  -p, --port=<portnum>	Set skale port number
  -k, --key=<acess_key>	Set skale access key
  -s, --ssl		Use SSL protocol
  -h, --help		Show help
  -V, --version		Show version
`;

const fs = require('fs');
const http = require('http');
const https = require('https');

var host = process.env.SKALE_HOST;
var port = process.env.SKALE_PORT;
var key = process.env.SKALE_KEY;
var ssl = process.env.SKALE_SSL;

const argv = require('minimist')(process.argv.slice(2), {
	string: ['H', 'host', 'k', 'key'],
	boolean: ['h', 'help', 'V', 'version', 's', 'ssl'],
});

if (argv.h || argv.help) {
	console.log(help);
	process.exit();
}
if (argv.V || argv.version) {
	var pkg = require('./package');
	console.log(pkg.name + '-' + pkg.version);
	process.exit();
}

host = argv.H || argv.host || die('Error: missing host');
port = argv.p || argv.port || port;
key = argv.k || argv.key || key;
ssl = argv.s || argv.ssl || ssl;
var proto = ssl ? require('https') : require('http');

switch (argv._[0]) {
	case 'run':
		run(argv._[1], argv._.splice(2));
		break;
	default:
		die('Error: invalid command: ' + argv._[0]);
}

function die(err) {
	console.error(help);
	console.error(err);
	process.exit(1);
}

function run(src, args) {
	fs.readFile(src, {encoding: 'utf8'}, function (err, data) {
		if (err) throw err;

		var postdata = JSON.stringify({src: data, args: args});

		var options = {
			hostname: host,
			port: port,
			path: '/run',
			method: 'POST',
			headers: {
				'X-Auth': key,
				'Content-Type': 'application/json',
				'Content-Length': Buffer.byteLength(postdata)
			}
		};

		var req = proto.request(options, function (res) {
			res.setEncoding('utf8');
			res.pipe(process.stdout);
		});

		req.on('error', function (err) {throw err;});
		req.end(postdata);
	});
}
