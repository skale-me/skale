#!/usr/local/bin/node --harmony
'use strict';

var ugrid = require('ugrid');
var co = require('co');

co(function *() {
	var uc = yield ugrid.context();
	var res = yield uc.textFile('/tmp/ugrid_test/emptyFile')
		.count();
	console.log(res);
	uc.end();
}).catch(ugrid.onError);