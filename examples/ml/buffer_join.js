#!/usr/local/bin/node --harmony
'use strict';

var ugrid = require('ugrid');
var MongoClient = require('mongodb').MongoClient;

require('child_process').execSync('rm -rf /tmp/ugrid/'); // Flush ugrid cluster tmp dir, WORKAROUND

ugrid.context(function(err, uc) {
    if (err) {console.log(err); process.exit();}
    console.log('# Connected to ugrid');

    // MongoClient.connect('mongodb://ugrid:luca-sas@172.17.0.20:27017/ugrid', function(err, db) {
    MongoClient.connect('mongodb://localhost:27017/ugrid', function(err, db) {
        if (err) {console.log(err); process.exit();}
        console.log('# Connected to mongodb database');

        // Query mongo using mongo native driver, return a streaming cursor, inject cursor inside ugrid cluster
        var collection = db.collection('buffer');
        var query = {type: 'invoice.payment_succeeded'};
        var projection = {"stripe_customer_id": 1, "value.data.object.lines.subscriptions": 1, _id: 0};
        var limit = 100000;
        // var limit = 0;

        console.log('# query = ' + JSON.stringify(query));
        console.log('# projection = ' + JSON.stringify(projection));

        function mapper(data) {
            return data.value.data.object.lines.subscriptions ? 
                [data.stripe_customer_id, data.value.data.object.lines.subscriptions] : null;
        }
        function filter(data) {return data != null;}

        var set1 = uc.objectStream(collection.find(query, projection).limit(limit)).map(mapper).filter(filter);
        var set2 = uc.objectStream(collection.find(query, projection).limit(limit)).map(mapper).filter(filter);

        set1.join(set2).groupByKey().count(function(err, res) {
            if (err) {console.log(err); process.exit();}
            console.log("# result");
            console.log(res)
            uc.end();       // close ugrid context
            db.close();     // close database connexion
        });
    });
});
