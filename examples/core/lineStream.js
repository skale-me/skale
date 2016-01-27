#!/usr/bin/env node

var fs = require('fs');
var sc = new require('skale').Context();

var stream = fs.createReadStream(__dirname + '/kv.data');

sc.lineStream(stream).collect().toArray(function(err, res) {
	console.log('Success !')
	console.log(res);
	sc.end();
});
