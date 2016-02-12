#!/usr/bin/env node

var assert = require('assert');
var uc = new require('ugrid').Context();

function reducer(a, b) {a.push(b); return a;}
function combiner(a, b) {return a.concat(b);}

var file = __dirname + '/kv.data';

var a = uc.textFile(file).aggregate(reducer, combiner, [], function(err, res) {
	res.sort();
	assert(JSON.stringify(res) === JSON.stringify(['1 1', '1 1', '2 3', '2 4', '3 5'])); 	
	console.log('Success !')
	console.log(res);
	uc.end();
})
