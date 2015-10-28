#!/usr/local/bin/node
'use strict';

var fs = require('fs');
var uc = new require('ugrid').Context();


// var file = "/Users/cedricartigue/work/ugrid/examples/less_than_a_line_with_4_workers";
var file = "/Users/cedricartigue/work/ugrid/examples/less_bytes_than_workers";
// var file = "/Users/cedricartigue/work/ugrid/examples/random";

var expected = fs.readFileSync(file, {encoding: 'utf8'});

var res = uc.textFile(file).collect();

var str = '';
res.on('data', function(data) {str += data + '\n'});
res.on('end', function() {
	str = str.substr(0, str.length - 1);
	if (str == expected) console.log('TEST OK !!');
	else {
		console.log('TEST FAILED !!');
		console.log('Expected : ')
		console.log(expected)

		console.log('\nresult: ')
		console.log(str);
	}
	uc.end();
});

