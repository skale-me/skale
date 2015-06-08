var spawn = require('child_process').spawn;
var assert = require('assert');
var ugrid = require('../');
var UgridClient = require('../lib/ugrid-client.js');

var server, workerController, uc, monitor;
var monitoringActions = [];

describe('ugrid server', function () {
	it('starts', function () {
		server = spawn('./bin/ugrid.js');
		assert(server.pid > 0);
	});

	var output;
	it('has some output', function (done) {
		server.stdout.on('data', function (data) {
			if (output) return;
			output = true;
			done();
		});
	});

	it('allows monitoring', function () {
		monitor = new UgridClient();
		monitor.subscribe('monitoring');
		monitor.on('monitoring', function (msg) {
			for (var i = 0; i < monitoringActions.length; i++)
				monitoringActions[i](msg);
		});
	});
});

describe('worker controller', function () {
	var hasConnect, hasOutput, hasPrefork;

	it('starts', function () {
		workerController = spawn('./bin/worker.js');
		assert(workerController.pid > 0);
	});

	it('connects to the server', function (done) {
		monitoringActions.push(function (msg) {
			if (!hasConnect && msg.data.event == 'connect' &&
				 msg.data.data.type == 'worker-controller') {
				hasConnect = true;
				done();
			}
		});
	});

	it('has some output', function (done) {
		workerController.stdout.on('data', function (data) {
			if (hasOutput) return;
			hasOutput = true;
			done();
		});
	});

	it('preforks workers', function (done) {
		monitoringActions.push(function (msg) {
			if (!hasPrefork && msg.data.event == 'connect' &&
				msg.data.data.type == 'worker') {
				hasPrefork = true;
				done();
			}
		});
	});
});
