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
console.log('line count on ' + file)

co(function *() {
	var uc = yield ugrid.context();
	console.log(yield uc.textFile(file).count());
	uc.end();
}).catch(ugrid.onError);
