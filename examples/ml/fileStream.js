#!/usr/local/bin/node --harmony
'use strict';

var co = require('co');
var fs = require('fs');
var ugrid = require('../..');
require('child_process').execSync('rm -rf /tmp/ugrid/');

co(function *() {
    var uc = yield ugrid.context();
    var N = 100;

    var s1 = fs.createReadStream('stream.data', {encoding: 'utf8'});
    var out = uc.lineStream(s1, {N: 4}).collect({stream: true});
    out.on('data', function(res) {console.log(res);});
    out.on('end', function(res) {uc.end();});
}).catch(ugrid.onError);
