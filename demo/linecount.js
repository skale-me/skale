#!/usr/local/bin/node --harmony
'use strict';

var co = require('co');
var ugrid = require('..');

var file = '/Users/cedricartigue/Desktop/stripe_events_bak.csv';

console.log('Processing file: ' + file);

co(function *() {
	var uc = yield ugrid.context();

	var sampled = yield uc.textFile(file)
		.sample(false, 0.01)
		.count();

	console.log(sampled);

	uc.end();
}).catch(ugrid.onError);
