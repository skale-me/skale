#!/usr/local/bin/node --harmony
'use strict';

var co = require('co');
var ugrid = require('../');
require('child_process').execSync('rm -rf /tmp/ugrid/');
/*
    raw data:       { cid: 'cus_ZVFeMy3pZ30mbb', period: { start: 1354318045, end: 1356910045 } }
    map:            [cid, period]
    groupByKey:     [cid, [period_0, ..., period_N_cid]]
*/
co(function *() {
    var uc = yield ugrid.context();

    // var e1 = new Date("June 30, 2014 23:59:59"), b1 = new Date(e1);
    // var e1 = new Date("July 31, 2014 23:59:59"), b1 = new Date(e1);
    // var e1 = new Date("August 31, 2014 23:59:59"), b1 = new Date(e1);
    // var e1 = new Date("September 30, 2014 23:59:59"), b1 = new Date(e1);
    // var e1 = new Date("October 31, 2014 23:59:59"), b1 = new Date(e1);
    // var e1 = new Date("November 30, 2014 23:59:59"), b1 = new Date(e1);
    // var e1 = new Date("December 31, 2014 23:59:59"), b1 = new Date(e1);
    // var e1 = new Date("January 31, 2015 23:59:59"), b1 = new Date(e1);
    // var e1 = new Date("February 28, 2015 23:59:59"), b1 = new Date(e1);
    // var e1 = new Date("March 31, 2015 23:59:59"), b1 = new Date(e1);
    // var e1 = new Date("April 30, 2015 23:59:59"), b1 = new Date(e1);
    // var e1 = new Date("May 31, 2015 23:59:59"), b1 = new Date(e1);
    // var e1 = new Date("June 30, 2015 23:59:59"), b1 = new Date(e1);
    // var e1 = new Date("July 31, 2015 23:59:59"), b1 = new Date(e1);
    var e1 = new Date("August 31, 2015 23:59:59"), b1 = new Date(e1);

    b1.setDate(1);
    b1.setHours(0, 0, 1);

    var e0 = new Date(e1);
    e0.setDate(0);
    var b0 = new Date(e0);
    b0.setDate(1);
    b0.setHours(0, 0, 1);

    console.log([b0, e0, b1, e1]);

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

    // var data = yield uc.textFile('/Users/cedricartigue/work/ugrid/demo/subscription.data')
    var data = yield uc.textFile('/Users/cedricartigue/work/ugrid/demo/payment_succeeded.data')
        .map(function (line) {
            var data = JSON.parse(line);
            return [data.cid, data.period];
        })
        .groupByKey()
        .aggregate(reducer, combiner, init);

    var rr = Math.round((data.kept_customers / data.customers) * 10000) / 100;
    var cr = Math.round((100 - rr) * 100) / 100;
    console.log('Churn rate on ' + new Date(init.e1 * 1000) + ' = ' + cr);

    uc.end();
}).catch(ugrid.onError);
