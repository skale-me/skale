#!/usr/local/bin/node --harmony

'use strict';

var co = require('co');
var coshell = require('co-shell');
var ugrid = require('../');

var webid = process.env.UGRID_WEBID;
var prompt = webid ? '' : 'ugrid> ';

co(function *() {
	var uc = yield ugrid.context({noworker: true});
	var context = coshell({prompt: prompt}).context;
	context.ugrid = ugrid;
	context.uc = uc;
	context.plot = webid ? function (data) {
		uc.send(0, {cmd: 'plot', id: webid, data: data});
	} : function() {};
}).catch(ugrid.onError);
