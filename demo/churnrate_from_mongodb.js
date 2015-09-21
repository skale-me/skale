#!/usr/local/bin/node --harmony
'use strict';

var co = require('co'), pg = require('pg'), thenify = require('thenify');
var MongoClient = require('mongodb').MongoClient;
var ugrid = require('../');
require('child_process').execSync('rm -rf /tmp/ugrid/');

var MongoConnect = thenify(MongoClient.connect);

co(function *() {
    var uc = yield ugrid.context();
    var db = yield MongoConnect('mongodb://localhost:27017/ugrid');
    var N = 561729;  // Hard limit to be released in ugrid stream engine

    var cursor = db.collection('buffer')
        .find({type: 'invoice.payment_succeeded'}, {"stripe_customer_id": 1, "value.data.object.lines.subscriptions": 1, _id: 0})
        .limit(N - 1);

    function mapper(data) {
        var cid = data.stripe_customer_id;
        try {
            var sub = data.value.data.object.lines.subscriptions[0].period;
        } catch (e) {return [cid, {start: 0, end: 0}]}
        return [cid, sub];
    }

    var e1 = new Date("August 31, 2015 23:59:59"), b1 = new Date(e1);

    b1.setDate(1);
    b1.setHours(0, 0, 1);

    var e0 = new Date(e1);
    e0.setDate(0);
    var b0 = new Date(e0);
    b0.setDate(1);
    b0.setHours(0, 0, 1);

    var init = {
        b0: b0.getTime() / 1000, e0: e0.getTime() / 1000,
        b1: b1.getTime() / 1000, e1: e1.getTime() / 1000,        
        customers: 0, kept_customers: 0
    };

    function reducer(acc, data) {
        // Did he paid for previous month ?
        var subs = data[1], customer_paid_one_month_earlier = false;
        for (var i = 0; i < subs.length; i++) {
            // Ignore annual plan for now
            var elapsed_time = (subs[i].end - subs[i].start) / (3600 * 24);
            var isMonthlyPlan = (elapsed_time <= 31);
            var isTrial = (elapsed_time <= 15);
            if (isTrial) continue;
            if (isMonthlyPlan) {
                if ((acc.b0 <= subs[i].start) && (subs[i].start <= acc.e0)) {
                    acc.customers++;
                    customer_paid_one_month_earlier = true;
                    break;
                }
            }
        }
        if (customer_paid_one_month_earlier) {
            // Ok he paid for previous month, but did he pay for this month too ?
            for (var i = 0; i < subs.length; i++) {
                var elapsed_time = (subs[i].end - subs[i].start) / (3600 * 24);
                var isMonthlyPlan = (elapsed_time <= 31);
                var isTrial = (elapsed_time <= 15);
                if (isTrial) continue;
                if (isMonthlyPlan) {
                    if ((subs[i].start >= acc.b1) && (subs[i].start <= acc.e1)) {
                        acc.kept_customers++;
                        break;
                    }
                }
            }
        }
        return acc;
    }

    function combiner(a, b) {
        a.customers += b.customers;
        a.kept_customers += b.kept_customers;
        return a;
    }

    var data = yield uc.objectStream(cursor, {N: N})
        .map(mapper)
        .groupByKey()
        .aggregate(reducer, combiner, init);

    var rr = Math.round((data.kept_customers / data.customers) * 10000) / 100;
    var cr = Math.round((100 - rr) * 100) / 100;
    console.log('Churn rate on ' + new Date(init.e1 * 1000) + ' = ' + cr);
    db.close();
    uc.end();
}).catch(ugrid.onError);
