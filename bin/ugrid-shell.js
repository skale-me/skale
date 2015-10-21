#!/usr/bin/env node

'use strict';

var co = require('co');
var coshell = require('co-shell');
var ugrid = require('../');

var webid = process.env.UGRID_WEBID;
var prompt = webid ? '' : 'ugrid> ';

co(function *() {
	var uc = yield ugrid.context();
	var r = coshell({
		prompt: prompt,
		ignoreUndefined: true,
		writer: function () {return undefined;}
	});
	r.context.ugrid = ugrid;
	r.context.uc = uc;
	r.context.plot = webid ? function (data) {
		uc.send(0, {cmd: 'plot', id: webid, data: data});
	} : function() {};
	r.on('exit', uc.end);
}).catch(ugrid.onError);
