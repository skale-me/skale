#!/usr/local/bin/node --harmony

'use strict';

var co = require('co');
var coshell = require('co-shell');
var UgridClient = require('../lib/ugrid-client.js');

co(function *() {
	var r = coshell({
		prompt: 'ugrid> ',
		ignoreUndefined: true,
		writer: function () {return undefined;}
	});
	r.context.UgridClient = UgridClient;
}).catch(function (err) {console.err(err.stack); process.exit(1);});
