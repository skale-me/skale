var vm = require('vm');
var thenify = require('thenify');
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

var read = thenify(function (s, callback) {
	s.resume();
	s.once('data', function(data) {
		s.pause();
		callback(null, data);
	});
});

co(function *() {
	var cmd;
	uc = yield ugrid.context({noworker: true});
	sandbox.uc = uc;
	trace('uc: %j', uc);
	//vm.createContext(sandbox);
	//vm.runInContext(initCode, sandbox);
	process.stdin.setEncoding('utf8');
	process.stdin.pause();
	while (true) {
		cmd = yield read(process.stdin);
		cmd = 'co.call(undefined, function *() {' + cmd + '})';
		eval(cmd);
		//vm.runInThisContext(cmd);
	}
	//process.stdin.on('data', function(data) {
		// data = 'co(function *() {' + data + '}).catch(ugrid.onError);';
		//vm.runInContext(data, sandbox);
	//);
}).catch(ugrid.onError);

	var plot = function plot(data) {
		uc.send(0, {cmd: "plot", id: webid, data: data});
	}
