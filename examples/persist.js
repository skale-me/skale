#!/usr/local/bin/node
'use strict';

var uc = new require('ugrid').Context();

var file = "/Users/cedricartigue/work/ugrid/examples/svm";

function mapper(line) {return line.split(' ').map(Number);}

function reducer(a, b) {
	a[0] += b.reduce(function(a, b) {return a + b});
	return a;
}

// var data = uc.textFile(file).map(mapper).persist();
// console.time('prepersist');
// data.count(function(err, result) {
// 	console.timeEnd('prepersist');
// 	console.log('count = %d\n', result);
// 	console.time('postpersist');
// 	data.count(function(err, result) {
// 		console.timeEnd('postpersist');
// 		console.log('count = %d\n', result);
// 		uc.end();
// 	});
// });

var data = uc.textFile(file).map(mapper).persist();
console.time('prepersist');
data.reduce(reducer, [0], function(err, result) {
	console.timeEnd('prepersist');
	console.log('count = %d\n', result[0]);
	console.time('postpersist');
	data.reduce(reducer, [0], function(err, result) {
		console.timeEnd('postpersist');
		console.log('count = %d\n', result[0]);
		uc.end();
	});
});