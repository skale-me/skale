#!/usr/local/bin/node --harmony
'use strict';

var co = require('co');
var ugrid = require('../..');
var execSync = require('child_process').execSync;

execSync('rm -rf /tmp/ugrid/');

co(function *() {
    var uc = yield ugrid.context();

    var nPartitions = 2;
    var N = 100;
    // var N = 1000000;
    var vect = [];
    for (var i = 0; i < N; i++) vect.push(i);
    // for (var i = 0; i < N; i++) vect.push(Math.floor(Math.random() * N));

    var vect = [1, 2, 1, 4, 2];
    var a = uc.parallelize(vect, nPartitions).distinct();
    var res = yield a.collect();

    console.log('initial ')
    console.log(vect)
    console.log('\n# distinct')
    console.log(res)

    uc.end();
}).catch(ugrid.onError);
