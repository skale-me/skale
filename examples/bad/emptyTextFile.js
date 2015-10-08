#!/usr/local/bin/node --harmony
'use strict';

var ugrid = require('ugrid');
var fs = require('fs');
var co = require('co');

// try {fs.mkdirSync('/tmp/ugrid_test');} catch (err) {;}
// try {fs.writeFileSync('/tmp/ugrid_test/emptyFile', '', 'utf8');} catch (err) {;}

// ugrid.context(function(err, uc) {
// 	uc.textFile('/tmp/ugrid_test/emptyFile').count(function(err, res) {
// 		if (err) {console.log(err); process.exit();}
// 		console.log('Number of lines in empty file = ' + res);
// 		uc.end();
// 	});
// });

co(function *() {
	var uc = yield ugrid.context();
	var res = yield uc.textFile('/tmp/ugrid_test/emptyFile').count();
	console.log(res);
	uc.end();
}).catch(ugrid.onError);