#!/usr/local/bin/node --harmony
'use strict';

var co = require('co'), thenify = require('thenify');
var MongoClient = require('mongodb').MongoClient;
var MongoConnect = thenify(MongoClient.connect);

var customers = {};
co(function *() {
    var db = yield MongoConnect('mongodb://localhost:27017/ugrid');                             // local use
    // var db = yield MongoConnect('mongodb://ugrid:luca-sas@172.17.0.20:27017/ugrid');        // mongodb server url inside docker cluster

    var cursor = db.collection('buffer')
        .find({type: 'invoice.payment_succeeded'}, {"stripe_customer_id": 1, "value.data.object.lines.subscriptions": 1, _id: 0})
        .limit(10);

    // Get user id and subscription periods, 
    cursor.on('data', function(data) {
        try {
            var id = data.stripe_customer_id;
            var sub = data.value.data.object.lines.subscriptions[0].period;
            if (customers[id] == undefined) customers[id] = {id: data.stripe_customer_id, subscriptions: []}
            customers[id].subscriptions.push(sub);
        } catch (e) {
            // console.log('bad data');
            return;
        }
        // console.log(customers[id])
    })

    // Put each customer in customers collection
    cursor.on('end', function() {
        console.log(customers)
        // // console.log(Object.keys(customers).length)
        // var l = 0, L = Object.keys(customers).length;
        // for (var c in customers) {
        //     db.collection('customers').insert(customers[c], function(err, result) {
        //         if (++l == L) db.close();
        //     });
        // }
    })
});
