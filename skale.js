#!/usr/bin/env node

// Copyright 2016 Luca-SAS, licensed under the Apache License 2.0

const help=`Usage: skale [options] <command> [<args>]

Commands:
  init				Set configuration
  run <file> [<args>...]	Run file on skale cluster
  demo <file> [<args>...]	Run demo file on skale cluster

Options:
  -c, --config=<file>	Set configuration file	[~/.skalerc]
  -H, --host=<hostname>	Set skale hostname	[SKALE_HOST] 
  -p, --port=<portnum>	Set skale port number	[SKALE_PORT]
  -k, --key=<acess_key>	Set skale access key	[SKALE_KEY]
  -s, --ssl		Use SSL protocol
  -h, --help		Show help
  -V, --version		Show version
`;

const fs = require('fs');

const argv = require('minimist')(process.argv.slice(2), {
	string: ['c', 'config', 'H', 'host', 'p', 'port', 'k', 'key'],
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

const config = load(argv);
const proto = config.ssl ? require('https') : require('http');

switch (argv._[0]) {
	case 'init':
		break;
	case 'demo':
		run(__dirname + '/' + argv._[1], argv._.splice(2));
		break;
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

function load(argv) {
	var conf = {}, save = false;
	var path = argv.c || argv.config || process.env.SKALE_CONFIG || process.env.HOME + '/.skalerc';
	try { conf = JSON.parse(fs.readFileSync(path)); } catch (error) { save = true; }
	conf.host = argv.H || argv.host || process.env.SKALE_HOST || conf.host || die('Error: missing host');
	conf.port = argv.p || argv.port || process.env.SKALE_PORT || conf.port || die('Error: missing port');
	conf.key = argv.k || argv.key || conf.key;
	conf.ssl = argv.s || argv.ssl || (conf.ssl ? true : false);
	if (save || argv._[0] == 'init') fs.writeFileSync(path, JSON.stringify(conf, null, 2));
	return conf;
}

function run(src, args) {
	fs.readFile(src, {encoding: 'utf8'}, function (err, data) {
		if (err) throw err;

		var postdata = JSON.stringify({src: data, args: args});

		var options = {
			hostname: config.host,
			port: config.port,
			path: '/run',
			method: 'POST',
			headers: {
				'X-Auth': config.key,
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
