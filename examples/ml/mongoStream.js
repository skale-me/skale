#!/usr/local/bin/node --harmony
'use strict';

var co = require('co');
var fs = require('fs');
var thenify = require('thenify');
var MongoClient = require('mongodb').MongoClient; 
var ugrid = require('ugrid');
require('child_process').execSync('rm -rf /tmp/ugrid/');

var MongoConnect = thenify(MongoClient.connect);

co(function *() {
    var uc = yield ugrid.context();
    var db = yield MongoConnect('mongodb://localhost:27017/ugrid');

    var query = {type: 'invoice.payment_succeeded'};
    var projection = {"stripe_customer_id": 1, "value.data.object.lines.subscriptions": 1, _id: 0};

    var cursor = db.collection('buffer').find(query, projection)
        // .limit(N);

    var out = uc.objectStream(cursor).count({stream: true});

    out.on('data', console.log);

    out.on('end', function(res) {
        db.close();
        uc.end();
    });

    // var out = uc.objectStream(cursor, {N: N}).collect({stream: true});

    // out.on('data', function(res) {
    //     console.log(res);
    // });

    // out.on('end', function(res) {
    //     db.close();
    //     uc.end();
    // });
}).catch(ugrid.onError);
