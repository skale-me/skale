#!/usr/bin/env node

var uc = new require('ugrid').Context();

var da0 = uc.parallelize([0, 1]);
var da1 = uc.parallelize([0, 1]);
var da2 = uc.parallelize([0, 1]);

function mapper2(data) {
	return [ data[0][0], data[0][1], data[1] ]
}

da0.cartesian(da1).cartesian(da2).map(mapper2).
    collect().on('data', console.log).on('end', uc.end);
