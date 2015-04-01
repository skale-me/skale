#!/usr/local/bin/node --harmony

var co = require('co');
var ugrid = require('../..');

co(function *() {
	var uc = yield ugrid.context();

	console.log(uc.worker.length);
	
	//var res = yield uc.devices({type: 'worker'});
	//console.log(res);
	uc.end();
}).catch(ugrid.onError);
