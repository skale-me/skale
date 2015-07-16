#!/usr/local/bin/node --harmony
'use strict';

var co = require('co');
var ugrid = require('../..');

// Example with graph cycles
// co(function *() {
//     var uc = yield ugrid.context();

//     // var a = uc.parallelize([1, 2]);
//     // var b = a.map(function(v) {return v * v});

//     var a = uc.parallelize([2, 3]);
//     var b = uc.parallelize([4, 9]);

//     var res = yield a.union(b).collect();
//     console.log(res);
//     uc.end();
// }).catch(ugrid.onError);

// Simple example
co(function *() {
    var uc = yield ugrid.context();

    var a = uc.parallelize([1, 2]);
    var res = yield a.map(function(v) {return v * v}).collect();

    console.log(res);
    uc.end();
}).catch(ugrid.onError);


// Example with shuffle node
// co(function *() {
//     var uc = yield ugrid.context();

//     var a = uc.parallelize([[1,1],[2,3],[2,4],[3,5]]).distinct();
//     var res = yield a.collect();

//     console.log(res);
//     uc.end();
// }).catch(ugrid.onError);