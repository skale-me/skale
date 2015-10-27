#!/usr/bin/env node

'use strict';

var fork = require('child_process').fork;
var Lines = require('../lib/lines.js');
var trace = require('line-trace');

var opt = require('node-getopt').create([
	['h', 'help', 'print this help text'],
	['H', 'Host=ARG', 'server hostname (default localhost)'],
	['P', 'Port=ARG', 'server port (default 12346)']
]).bindHelp().parseSystem();

var host = opt.options.Host || 'localhost';
var port = opt.options.Port || 12346;
var shells = {};

var ugrid = require('../lib/ugrid-client.js')({
	host: host,
	port: port,
	data: {type: 'controller'}
});

console.log('app controller ready');

//ugrid.on('start', function (msg) {
//	var cmd = __dirname + '/../examples/web/' + msg.data.app + '.js';
//	var prog = fork(cmd, [JSON.stringify(msg.data)], {silent: true});
//	prog.stdout.on('data', function(data) {
//		console.log('# stdout: ' + data);
//	});
//	prog.stderr.on('data', function(data) {
//		console.log('# stderr: ' + data);
//	});
//	prog.on('close', function (code) {
//		console.log('child process exited with code ' + code);
//	});
//});

ugrid.on('remoteClose', function (msg) {
	if (msg.data in shells) {
		console.log('remoteClose ' + msg.data + ', terminate ' + shells[msg.data].pid);
		//shells[msg.data].kill();
		//delete shells[msg.data];
	}
});

var sessions = {};

ugrid.on('shell', function (msg) {
	var lines = new Lines(), shell, cwd;
	if (sessions[msg.webid]) {
		trace('reconnect attempt');
		// Reconnect to an existing shell
		ugrid.send(0, {cmd: 'shell', id: msg.from});
		ugrid.send(0, {cmd: 'notify', data: msg.data});
		shell = sessions[msg.webid];
		shell.stdout.pipe(lines);
		shell.stdin.write(JSON.stringify({data: 'webid = "' + msg.webid + '"; dest = "' + msg.from + '";'}) + '\n');
		lines.on('data', outputCmd);
		return;
	}
	// Create a new shell
	process.env.UGRID_WEBID = msg.webid;
	cwd = './users/' + msg.webid.replace(":", "/");
	shell = fork(__dirname + '/../lib/copro.js', {silent: true, cwd: cwd});
	shells[msg.data] = shell;
	sessions[msg.webid] = shell;
	ugrid.send(0, {cmd: 'shell', id: msg.from});
	ugrid.send(0, {cmd: 'notify', data: msg.data});
	shell.stdout.pipe(lines);

	lines.on('data', outputCmd);

	shell.stderr.on('data', function (data) {
		console.log("# shell pid %d stderr: %s", shell.pid, data);
	});
	shell.on('close', function (code) {
		console.log("# shell %d exited with code: %d", shell.pid, code);
	});
	ugrid.on('stdin-' + msg.webid, inputCmd);
	console.log('forked ugrid-shell.js pid ' + shell.pid);

	function inputCmd(cmd) {
		shell.stdin.write(JSON.stringify(cmd) + '\n');
	}

	function outputCmd(data) {
		data = JSON.parse(data);
		if (data.plot)
			ugrid.send(0, {cmd: 'plot-' + msg.webid, id: msg.from, data: data.plot, file: data.file});
		else
			ugrid.send(0, {cmd: 'stdout-' + msg.webid, id: msg.from, file: data.file, data: data.data + '\n'});
	}
});
