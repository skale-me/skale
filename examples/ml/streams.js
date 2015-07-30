#!/usr/local/bin/node --harmony
'use strict';

var co = require('co');
var fs = require('fs');
var ugrid = require('../..');

co(function *() {
    var uc = yield ugrid.context();

    // var s1 = process.stdin;
    // var s2 = process.stdin;
    // var s2 = fs.createReadStream('rank.data', {encoding: 'utf8'});

    uc.lineStream(fs.createReadStream('rank.data', {encoding: 'utf8'}), {N: 2})
    	.collect({stream: true})
    	.on('data', console.log)
    	.on('end', uc.end);

    // uc.lineStream(s2, {N: 2})
    // 	.count({stream: true})
    // 	.on('data', console.log)
    // 	.on('end', uc.end);

    uc.startStreams();
}).catch(ugrid.onError);
