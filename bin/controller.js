#!/usr/local/bin/node --harmony

'use strict'

var spawn = require('child_process').spawn;

var opt = require('node-getopt').create([
	['h', 'help', 'print this help text'],
	['H', 'Host=ARG', 'server hostname (default localhost)'],
	['P', 'Port=ARG', 'server port (default 12346)']
]).bindHelp().parseSystem();

var host = opt.options.Host || 'localhost';
var port = opt.options.Port || 12346;

var ugrid = require('../lib/ugrid-client.js')({
	host: host,
	port: port,
	data: {type: 'controller'}
});

ugrid.on('start', function(res) {
	var cmd = __dirname + '/../examples/web/' + res.data.app + '.js';
	var prog = spawn('/usr/local/bin/node', ['--harmony', cmd, JSON.stringify(res.data)]);
	prog.stdout.on('data', function(data) {
		console.log('# stdout: ' + data);
	});
	prog.stderr.on('data', function(data) {
		console.log('# stderr: ' + data);
	});
	prog.on('close', function (code) {
		console.log('child process exited with code ' + code);
	});
});
