#!/usr/local/bin/node --harmony
'use strict';

var co = require('co');
var ugrid = require('../..');

co(function *() {
    var uc = yield ugrid.context();

    var nPartitions = 2;
    var N = 10000;
    var vect = [];
    // for (var i = 0; i < N; i++) vect.push(i);
    for (var i = 0; i < N; i++) vect.push(Math.floor(Math.random() * N));

    // var vect = [1, 2, 1, 4, 2];
    var a = uc.parallelize(vect, nPartitions).distinct();
    var res = yield a.count();
    console.log(res)

    uc.end();
}).catch(ugrid.onError);
