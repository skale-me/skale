#!/usr/bin/env node

var fs = require('fs');
var uc = new require('ugrid').Context();

var s2 = uc.lineStream(fs.createReadStream('/etc/hosts', 'utf8')).distinct().persist();

s2.collect().toArray(function(err, res) {
	console.log(res);
	console.log('First action before persist done\n')

	s2.collect().toArray(function(err, res) {
		console.log(res);
		console.log('Finished second action after persist')
		uc.end();
	})
})
