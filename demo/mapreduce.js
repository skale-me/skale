#!/usr/local/bin/node --harmony
'use strict';

var ugrid = require('ugrid');

ugrid.context(function(err, uc) {
	var vect = [1, 2, 3, 4, 5];

	function mapper(a) {return a * a;}

	// function sum(a, b) {return a + b;}
	function sum(a, b) {return a + b;}

	function done(err, result) {
		if (err) {console.log(err); process.exit(1)};
		console.log('Summing the squared value of [' + vect + '] gives ' + result);
		uc.end();
	}

	uc.parallelize(vect).map(mapper).reduce(sum, 0, done);
});


// var ugrid = require('ugrid');
// var co = require('co');

// co(function *() {
//     var uc = yield ugrid.context();
// 	var vect = [1, 2, 3, 4, 5];

// 	function mapper(a) {return a * a;}

// 	function reducer(a, b) {return a + b;}

// 	var res = yield uc.parallelize(vect).map(mapper).reduce(reducer, 0);

// 	console.log('Summing the squared value of [' + vect + '] gives ' + res);
// 	uc.end();
// }).catch(ugrid.onError);
