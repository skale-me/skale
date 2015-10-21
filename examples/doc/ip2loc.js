#!/usr/local/bin/node --harmony
'use strict';

var ugrid = require('ugrid');

ugrid.context(function(err, uc) {
	if (err) {console.log(err); process.exit();}

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

	function mapper(data, arg, wc) {
		if (wc.maxmind == undefined) {
			wc.maxmind = wc.require('maxmind');
			wc.maxmind.init(process.env.HOME + '/Downloads/GeoIP.dat');
		}
		return wc.maxmind.getCountry(data);
	}

	var res = uc.parallelize(vect).map(mapper).collect();

    res.on('data', console.log);
	res.on('end', uc.end);
});