#!/usr/bin/env node

var sc = new require('skale').Context();

var da1 = sc.parallelize([[10, 1], [20, 2]]);
var da2 = sc.parallelize([[10, 'world'], [30, 3]]);
var res = da1.rightOuterJoin(da2).collect()
res.on('data', console.log);
res.on('end',function(){    //expected // [ 10, [ 1, 'world' ] ]  // [ 30, [ null, 3 ] ]
	sc.end();
});	
