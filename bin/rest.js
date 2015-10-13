#!/usr/bin/env node

'use strict';

var child_process = require('child_process');
var fs = require('fs');
var trace = require('line-trace');
var tmp = require('tmp');

var express = require('express');
var bodyParser = require('body-parser');
var morgan = require('morgan');
var app = express();

app.use(bodyParser.urlencoded({extended: true}));
app.use(bodyParser.json());
app.use(morgan('dev'));

// Start web server
var webServer = app.listen(8000, function () {
	var addr = webServer.address();
	trace('webserver listening at %j', addr);
});

app.get('/', function (req, res) {res.send('Hello from ugrid server\n');});

app.get('/test', function (req, res) {
	trace(req.query)
	req.query.from = "ugrid get test";
	res.json(req.query);
});

app.post('/test', function (req, res) {
	trace(req.body)
	req.body.from = "ugrid post test";
	res.json(req.body);
});

// Exec a npm install command for master and workers
app.post('/install', function (req, res) {
	try {
		var child = child_process.spawn('npm', ['install', req.body.pkg])
		child.stderr.pipe(res);
		child.stdout.pipe(res);
	} catch (err) {
		res.status(500).send('installed failed on server: ' + err.message + '\n');
	}
});

// Exec a master from an already existing file
app.post('/exec', function (req, res) {
	try {
		var child = child_process.spawn(req.body.src, req.body.args);
		child.stderr.pipe(res);
		child.stdout.pipe(res);
	} catch (err) {
		res.status(500).send('exec failed on server: ' + err.message + '\n');
	}
});

// Exec a master using src embedded in request. A temporary file is used.
app.post('/run', function (req, res) {
	var name = tmp.tmpNameSync({template: __dirname + '/tmp/XXXXXX.js'});
	req.setTimeout(0);
	fs.writeFile(name, req.body.src, {mode: 493}, function (err) {
		if (err) return res.send({err: 1, stdout: null, stderr: 'write failed on server: ' + err.message});
		try {
			var child = child_process.spawn(name, req.body.args);
			child.stderr.pipe(res);
			child.stdout.pipe(res);
		} catch (err) {
			res.status(500).send('exec failed on server: ' + err.message + '\n');
		}
	});
});
