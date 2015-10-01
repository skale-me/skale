#!/usr/local/bin/node --harmony
'use strict';

/*
    This script extract the customers date of birth
*/

var co = require('co'), thenify = require('thenify');
var MongoClient = require('mongodb').MongoClient;
var MongoConnect = thenify(MongoClient.connect);

var customers = {};
co(function *() {
    var db = yield MongoConnect('mongodb://localhost:27017/ugrid');                             // local use
    // var db = yield MongoConnect('mongodb://ugrid:luca-sas@172.17.0.20:27017/ugrid');        // mongodb server url inside docker cluster

    var cursor = db.collection('buffer')
        .find({type: 'customer.created'}, {"value.data.object.id": 1, "value.created": 1, _id: 0})
        // .limit(10);

    // Get user id and date of birth
    cursor.on('data', function(data) {
        try {
            var id = data.value.data.object.id;
            var dob = data.value.created;
            if (customers[id] == undefined) customers[id] = {id: id, dob: dob}
        } catch (e) {return;}
    })

    // Put each customer in customers collection
    cursor.on('end', function() {
        console.log('DOB of all customers ready, now inserting')
        var l = 0, L = Object.keys(customers).length;
        for (var id in customers) {
            // find customer with id and add date of birth
            db.collection('customers').update({id: id}, {$set: {dob: customers[id].dob}}, function(err, result) {
                if (++l == L) db.close();
            })
        }
    })
});
