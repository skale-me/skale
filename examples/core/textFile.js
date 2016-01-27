#!/usr/bin/env node

var sc = new require('skale').Context();

function reducer(a, b) {a.push(b); return a;}
function combiner(a, b) {return a.concat(b);}

var file = __dirname + '/kv.data';

var a = sc.textFile(file).aggregate(reducer, combiner, [], function(err, res) {
	console.log('Success !')
	console.log(res);
	sc.end();
})
