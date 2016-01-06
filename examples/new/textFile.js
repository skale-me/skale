#!/usr/bin/env node

var uc = new require('ugrid').Context();

function reducer(a, b) {a.push(b); return a;}
function combiner(a, b) {return a.concat(b);}

var file = '/Users/cedricartigue/work/ugrid/examples/new/kv.data';

var a = uc.textFile(file).aggregate(reducer, combiner, [], function(err, res) {
	console.log('Success !')
	console.log(res);
	uc.end();
})
