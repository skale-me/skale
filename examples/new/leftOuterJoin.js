#!/usr/bin/env node

var uc = new require('ugrid').Context();

var da1 = uc.parallelize([[10, 1], [20, 2]]);
var da2 = uc.parallelize([[10, 'world'], [30, 3]]);
var res = da1.leftOuterJoin(da2).collect()
res.on('data', console.log);
res.on('end',function(){    //expected // [ 10, [ 1, 'world' ] ]  // [ 20, [ 2, null ] ]
			uc.end();
});	
