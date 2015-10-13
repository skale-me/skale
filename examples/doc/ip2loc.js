#!/usr/local/bin/node --harmony
'use strict';

var ugrid = require('ugrid');

ugrid.context(function(err, uc) {
	if (err) {console.log(err); process.exit();}
	console.log('Connected to ugrid');

	var vect = ['91.200.13.100',
    '31.222.157.220',
    '31.222.157.220',
    '95.211.87.85',
    '31.204.152.111',
    '216.151.130.170',
    '31.222.157.220',
    '54.219.142.216',
    '54.253.115.57',
    '216.151.130.170'];

  	var obj = {};	// objet remplaçant

	function mapper(data, arg, wc) {
		if (wc.trace == undefined) {
			console.log('Requiring package only once');
			wc.trace = wc.require('line-trace');
			// var mmdb = wc.require('mmdb');
			// wc.reader = new mmdb('~/GeoIP2-City_20151006/GeoIP2-City.mmdb');
		}
		// return wc.reader.lookup(data).country.iso_code;		
		wc.trace('toto');
		return data;
	}

	uc.parallelize(vect)
    	.map(mapper, obj)
    	.collect(done);

	function done(err, res) {
    	console.log('Done');
		if (err) {console.log('err'); process.exit(1)};
		for (var i = 0; i < res.length; i++) {
			console.log('Res #' + i);
			console.log(res[i]);
    	}
		uc.end();
	}
});