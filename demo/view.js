#!/usr/local/bin/node --harmony
'use strict';

var co = require('co');
var assert = require('assert');
var thenify = require('thenify');
var MongoClient = require('mongodb').MongoClient;
var MongoConnect = thenify(MongoClient.connect);

co(function *() {
    var db = yield MongoConnect('mongodb://localhost:27017/ugrid');

    var cursor = db.collection('connexions').find({}, {_id: false});
    // var cursor = db.collection('customers').find({}, {_id: false});
    cursor.on('data', function(res) {
        console.log(res);
    })
    cursor.on('end', function(res) {
        db.close();
    });
});
