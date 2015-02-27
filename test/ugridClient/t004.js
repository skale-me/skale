#!/usr/local/bin/node --harmony

// Test co / yield

var co = require('co');
var grid = require('../../lib/ugrid-client.js')({data: {type: 't000'}});

var closed = false;

process.on('exit', function () {
	console.assert(closed);
	console.assert(grid.id !== undefined);
});

grid.on('close', function () {closed = true;});

grid.on('request', function (msg) {
	grid.reply(msg, null, 'hello ' + msg.data);
});

co(function *() {
	var devices = yield grid.devices({type: 't000'});
	console.assert(devices[0] != undefined);
	var id = yield grid.send(0, {cmd: 'id', data: grid.uuid});
	console.assert(id == grid.id);
	var response = yield grid.request(devices[0], 'test');
	console.assert(response == 'hello test');
	var data = yield grid.get(grid.uuid);
	console.assert(data.type === 't000');
	grid.set({type: 't001', state: 'ok'});
	data = yield grid.get(grid.uuid);
	console.log(data);
	grid.end();
})();
