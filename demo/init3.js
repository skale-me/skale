#!/usr/local/bin/node --harmony
'use strict';

var co = require('co');
var assert = require('assert');
var thenify = require('thenify');
var MongoClient = require('mongodb').MongoClient;
var MongoConnect = thenify(MongoClient.connect);

var target_churn_rate = 0.1;
var lambda = 30;
var nCustomers = 1000;
var now = new Date();
var oneYearAgo = new Date();
oneYearAgo.setFullYear(now.getFullYear() - 1);

function insertRandomCustomer(db, id, callback) {
    var date = new Date(oneYearAgo), connexions = [];
    while (date < now) {
        var day = Math.ceil(Math.random() * (lambda / (1 - target_churn_rate) ));
        var delta = (day < lambda) ? day : lambda;
        date.setHours(date.getHours() + (24 * delta));
        if (day < lambda) {
            date.setHours(date.getHours() + (24 * day));
            connexions.push({customer_id: id, date: new Date(date)});
        } else date.setHours(date.getHours() + (24 * lambda));
    }

    if (connexions.length == 0) {return callback()};

    db.collection('connexions').insert(connexions, function(err, result) {
        assert.equal(err, null);
        assert.equal(connexions.length, result.result.n);
        assert.equal(connexions.length, result.ops.length);
        console.log("SUCCESSFULLY inserted connexions of customer " + id);
        callback(result);
    });
}

var n = 0;
co(function *() {
    var db = yield MongoConnect('mongodb://localhost:27017/ugrid');
    function done() {
        if (n < nCustomers) insertRandomCustomer(db, n++,  done);
        else db.close();
    }
    db.collection('connexions').remove(function() {insertRandomCustomer(db, n++, done);});
});