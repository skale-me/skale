#!/usr/local/bin/node --harmony

'use strict';

var co = require('co');
var coshell = require('co-shell');
var ugrid = require('../');

co(function *() {
	var uc = yield ugrid.context();
	//var context = coshell({prompt: 'ugrid> '}).context;
	var context = coshell({prompt: ''}).context;
	context.ugrid = ugrid;
	context.uc = uc;
	context.plot = function (data) {
		uc.send(0, {cmd: 'plot', id: process.env.UGRID_WEBID, data: data});
	};
	process.stdout.write("Welcome to ugrid-shell");
}).catch(ugrid.onError);
