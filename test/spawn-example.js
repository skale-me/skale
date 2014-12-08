#!/usr/local/bin/node --harmony

var spawn = require('child_process').spawn;
var prog = spawn('node', ['target.js']);

prog.stdout.on('data', function (data) {
	console.log('stdout: ' + data);
});

prog.stderr.on('data', function (data) {
	console.log('stderr: ' + data);
});

prog.on('close', function (code) {
	console.log('child process exited with code ' + code);	
});
