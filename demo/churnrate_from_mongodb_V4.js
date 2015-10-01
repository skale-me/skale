#!/usr/local/bin/node --harmony
'use strict';

var co = require('co'), thenify = require('thenify');
var MongoClient = require('mongodb').MongoClient;
// var ugrid = require('../');      // local use

var ugrid = require('../..');

require('child_process').execSync('rm -rf /tmp/ugrid/');
var MongoConnect = thenify(MongoClient.connect);

co(function *() {
    var uc = yield ugrid.context();
    // var db = yield MongoConnect('mongodb://localhost:27017/ugrid');
    var db = yield MongoConnect('mongodb://ugrid:luca-sas@172.17.0.20:27017/ugrid');        // mongodb server url inside docker cluster
    var N = 98726;  // Hard limit to be released in ugrid stream engine

    var cursor = db.collection('customers').find({}, {_id: 0}).limit(N - 1);
        // .limit(1000);

    var init = [];
    var end = new Date("August 31, 2015 23:59:59");
    for (var m = 0; m < 365 * 3; m++) {
        var start = new Date(end);
        start.setHours(0, 0, 1);
        init.unshift({start: new Date(start).getTime() / 1000, end: new Date(end).getTime() / 1000, customers: 0, kept_customers: 0});
        end.setDate(end.getDate() - 1);
        end.setHours(23, 59, 59);
    }

    function reducer(period, data) {
        // var subscription = data[1];
        var subscription = data.subscriptions;
        for (var i = 0; i < subscription.length; i++) {
            var elapsed_time = (subscription[i].end - subscription[i].start) / (3600 * 24);  // Duration of subscription period in days 
            if ((elapsed_time <= 15) || (elapsed_time > 31)) continue;       // skip trial and annual plans for now
            for (var j = 0; j < period.length; j++) {
                if (((period[j].start < subscription[i].start) && (subscription[i].start < period[j].end)) ||
                    ((period[j].start < subscription[i].end) && (subscription[i].end < period[j].end)) ||
                    ((subscription[i].start < period[j].start) && (period[j].end < subscription[i].end)) ) {
                    period[j].customerIsActive = true;
                }
            }
        }
        for (var i = 30; i < period.length; i++) {
            if (period[i - 30].customerIsActive) {
                period[i].customers++;
                if (period[i].customerIsActive) period[i].kept_customers++;
            }
        }
        for (var i = 0; i < period.length; i++) period[i].customerIsActive = undefined;

        return period;
    }

    function combiner(a, b) {
        for (var i = 0; i < a.length; i++) {
            a[i].customers += b[i].customers;
            a[i].kept_customers += b[i].kept_customers;
        }
        return a;
    }

    var data = yield uc.objectStream(cursor, {N: N}).aggregate(reducer, combiner, init);
    // var data = yield uc.objectStream(cursor, {N: N}).map(mapper).groupByKey().aggregate(reducer, combiner, init);

    // console.log(data)
    console.log('Monthly churn rate up to August 31, 2015 23:59:59')
    for (var i = 30; i < data.length; i++) {
        var rr = Math.round((data[i].kept_customers / data[i].customers) * 10000) / 100;
        var cr = Math.round((100 - rr) * 100) / 100;
        console.log(String(cr).replace('.', ','))
    }
    db.close();
    uc.end();
}).catch(ugrid.onError);
