#!/usr/local/bin/node --harmony
'use strict';

var ugrid = require('ugrid');
var co = require('co');

var uc = ugrid.context();

co(function *() {
	var res = yield uc.textFile('/tmp/ugrid_test/emptyFile')
		.count();
	console.log(res);
	uc.end();
}).catch(ugrid.onError);
