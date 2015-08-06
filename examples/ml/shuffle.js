#!/usr/local/bin/node --harmony
'use strict';

var co = require('co');
var ugrid = require('../..');

require('child_process').execSync('rm -rf /tmp/ugrid/');

co(function *() {
    var uc = yield ugrid.context();

    console.log('RandomSVMData ReduceByKey')

    // var N = 1000 * 1000 * 10;
    var D = 10;
    var seed = 1;
    var P = 2;

    var a = uc.randomSVMData(N, D, seed, P);
    // var res = yield a.collect();
    // console.log(res)

    function reducer(a, b) {
        for (var i = 0; i < b.length; i++)
            a[0] += b[i];
        return a;
    }
    var b = a.reduceByKey(reducer, [0]);
    var res = yield b.collect();
    console.log(res)

    uc.end();
}).catch(ugrid.onError);
