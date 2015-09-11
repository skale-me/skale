#!/usr/local/bin/node --harmony
'use strict';

var co = require('co');
var ugrid = require('../..');

var opt = require('node-getopt').create([
	['h', 'help', 'print this help text'],
	['f', 'F=ARG', 'Text input file'],
]).bindHelp().parseSystem();

var file = opt.options.F;
if (file == undefined) {console.log('You must provide an input file'); process.exit(0)};
console.log('line collectt on ' + file)

co(function *() {
	var uc = yield ugrid.context();
	var res = yield uc.textFile(file).collect();
	console.log(res);
	uc.end();
}).catch(ugrid.onError);
