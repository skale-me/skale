'use strict';

var vm = require('vm');
var co = require('co');
var ugrid = require('../');
var trace = require('line-trace');
var webid = process.env.UGRID_WEBID;
var uc;

var sandbox = {
	co: co,
	uc: uc,
	ugrid: ugrid,
	webid: webid,
	trace: trace,
	console: console,
	process: process
};

var initCode =
	'trace("uc: %j", uc);' +
	'function plot(data) {' +
		'uc.send(0, {cmd: "plot", id: webid, data: data});' +
	'}';
	// +
	// 'co(function *() {' +
	// 	'uc = yield ugrid.context({noworker: true});' +
	// '}).catch(ugrid.onError);';

co(function *() {
	uc = yield ugrid.context({noworker: true});
	sandbox.uc = uc;
	trace('uc: %j', uc);
	vm.createContext(sandbox);
	vm.runInContext(initCode, sandbox);
	process.stdin.setEncoding('utf8');
	process.stdin.on('data', function(data) {
		// data = 'co(function *() {' + data + '}).catch(ugrid.onError);';
		vm.runInContext(data, sandbox);
	});
}).catch(ugrid.onError);
