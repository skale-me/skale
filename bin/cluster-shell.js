#!/usr/local/bin/node --harmony

'use strict';

var co = require('co');
var coshell = require('co-shell');
var Cluster = require('../lib/ugrid-cluster.js');

co(function *() {
	var r = coshell({
		prompt: 'cluster> ',
		ignoreUndefined: true,
		writer: function () {return undefined;}
	});
	r.context.Cluster = Cluster;
}).catch(function (err) {console.err(err.stack); process.exit(1);});
