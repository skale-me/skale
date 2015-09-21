#!/usr/local/bin/node --harmony
'use strict';

var co = require('co');
var assert = require('assert');
var thenify = require('thenify');
var MongoClient = require('mongodb').MongoClient;
var MongoConnect = thenify(MongoClient.connect);

var target_churn_rate = 0.5;
var lambda = 30;
var now = new Date();
var oneYearAgo = new Date();
oneYearAgo.setFullYear(now.getFullYear() - 1);

function insertRandomCustomer(db, N, callback) {
    var customers = [];

    for (var i = 0; i < N; i++) {
        var date = new Date(oneYearAgo);
        var connexions = [];
        while (date < now) {
            var day = Math.round(Math.random() * (lambda * (1 + target_churn_rate)));
            var delta = (day < lambda) ? day : lambda;
            date.setHours(date.getHours() + (24 * delta));
            if (day < lambda) {
                date.setHours(date.getHours() + (24 * day));
                connexions.push(date.toString());
            } else date.setHours(date.getHours() + (24 * lambda));
        }
        customers[i] = {id: i, connexions: connexions};
    }

    db.collection('customers').insert(customers, function(err, result) {
        assert.equal(err, null);
        assert.equal(customers.length, result.result.n);
        assert.equal(customers.length, result.ops.length);
        console.log("Inserted " + N + " random customers into the customer collection");
        callback(result);
    });
}

co(function *() {
    var db = yield MongoConnect('mongodb://localhost:27017/ugrid');
    var nCustomers = 10;
	db.collection('customers').remove(function() {
	    insertRandomCustomer(db, nCustomers, function(result) {
	        db.close();
	    });		
	});
});