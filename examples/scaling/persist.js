#!/usr/local/bin/node --harmony
'use strict';

var co = require('co');
var ugrid = require('../..');
var sizeOf = require('../../utils/sizeof.js');

require('child_process').execSync('rm -rf /tmp/ugrid/');

// var N = 2000000;                // 1M environ 256 MB
var N = 1000 * 1000 * 8 * 4;       // equivalent à un fichier d'environ 6 Gb sur disque
var D = 10;
var seed = 1;
var P = 100;
var sample = [1, []];
for (var i = 0; i < D; i++) sample[1].push(Math.random());
var approx_data_size = N * sizeOf(sample);

console.log('# Scalability test on data persistency')
console.log('\tInput data: Random SVM Data');
console.log('\tNumber of observations: ' + N);
console.log('\tFeatures per observation: ' + D);
console.log('\tApproximate dataset size: ' + Math.ceil(approx_data_size / (1024 * 1024)) + ' Mb');

co(function *() {
    var uc = yield ugrid.context();

    var a = uc.randomSVMData(N, D, seed, P).persist();
    console.time('\nFirst iteration');
    var res = yield a.count();
    console.timeEnd('\nFirst iteration');
    console.log(res)

    console.time('\nSecond iteration');
    var res = yield a.count();
    console.timeEnd('\nSecond iteration');
    console.log(res)

    uc.end();
}).catch(ugrid.onError);
