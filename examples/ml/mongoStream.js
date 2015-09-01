#!/usr/local/bin/node --harmony
'use strict';

var co = require('co');
var fs = require('fs');
var thenify = require('thenify');
var MongoClient = require('mongodb').MongoClient; 
var ugrid = require('../..');
require('child_process').execSync('rm -rf /tmp/ugrid/');

var MongoConnect = thenify(MongoClient.connect);

co(function *() {
    var uc = yield ugrid.context();
    var db = yield MongoConnect('mongodb://localhost:27017/ugrid');
    var N = 100;

    var cursor = db.collection('ugrid').find({}, {_id: false});
    var out = uc.objectStream(cursor, {N: N}).collect({stream: true});

    out.on('data', function(res) {
        console.log(res);
    });

    out.on('end', function(res) {
        db.close();
        uc.end();
    });
}).catch(ugrid.onError);
