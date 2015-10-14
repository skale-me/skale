#!/usr/bin/env node

'use strict';

var child_process = require('child_process');
var fs = require('fs');
var trace = require('line-trace');
var tmp = require('tmp');

var bodyParser = require('body-parser');
var busboy = require('connect-busboy');
var express = require('express');
var morgan = require('morgan');

var app = express();
app.use(bodyParser.urlencoded({extended: true}));
app.use(bodyParser.json());
app.use(busboy());
app.use(morgan('dev'));

var access = process.env.UGRID_ACCESS;

// Start web server
var webServer = app.listen(8000, function () {
	var addr = webServer.address();
	trace('webserver listening at %j', addr);
});

function authenticate(req, res, next) {
	console.log(req)
	if (!access || access == req.body.access || access == req.query.access)
		return next();
	res.status(403).send('Invalid access key\n');
}

app.get('/', authenticate, function (req, res) {
	res.send('Hello from ugrid server\n');
});

app.get('/test', authenticate, function (req, res) {
	trace(req.query)
	req.query.from = "ugrid get test";
	res.json(req.query);
});

app.post('/test', authenticate, function (req, res) {
	trace(req.body)
	req.body.from = "ugrid post test";
	res.json(req.body);
});

// Exec a npm install command for master and workers
app.post('/install', authenticate, function (req, res) {
	try {
		var child = child_process.spawn('npm', ['install', req.body.pkg])
		child.stderr.pipe(res);
		child.stdout.pipe(res);
	} catch (err) {
		res.status(500).send('installed failed on server: ' + err.message + '\n');
	}
});

// Upload a data file from client site
app.post('/upload', function (req, res) {
	var isAuthenticated = (access == undefined);
	var uploading = false;
	req.pipe(req.busboy);
	req.busboy.on('field', function (key, value) {
		if (key == 'access' && access && value == access)
			isAuthenticated = true;
	});
	req.busboy.on('file', function (fieldname, file, filename) {
		if (!isAuthenticated) return res.status(403).send('invalid access key\n');
		trace('uploading ' + filename);
		uploading = true;
		var fstream = fs.createWriteStream(__dirname + '/tmp/' + filename);
		file.pipe(fstream);
		fstream.on('close', function () {
			res.send('uploaded ' + filename + '\n');
		});
	});
});

// Exec a master from an already existing file
app.post('/exec', authenticate, function (req, res) {
	try {
		var child = child_process.spawn(req.body.src, req.body.args);
		child.stderr.pipe(res);
		child.stdout.pipe(res);
	} catch (err) {
		res.status(500).send('exec failed on server: ' + err.message + '\n');
	}
});

// Exec a master using src embedded in request. A temporary file is used.
app.post('/run', authenticate, function (req, res) {
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
