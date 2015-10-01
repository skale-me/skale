#!/usr/local/bin/node --harmony
'use strict';

var co = require('co'), thenify = require('thenify');
var MongoClient = require('mongodb').MongoClient;
var fs = require('fs');
var ugrid = require('../');      // local use
// var ugrid = require('../..');

require('child_process').execSync('rm -rf /tmp/ugrid/');
var MongoConnect = thenify(MongoClient.connect);

co(function *() {
    var uc = yield ugrid.context();
    var db = yield MongoConnect('mongodb://localhost:27017/ugrid');
    // var db = yield MongoConnect('mongodb://ugrid:luca-sas@172.17.0.20:27017/ugrid');        // mongodb server url inside docker cluster
    var N = 98726;  // Hard limit to be released in ugrid stream engine

    var init = [], end = new Date("August 31, 2015 23:59:59");
    for (var m = 0; m < 31; m++) {
        var start = new Date(end);
        start.setHours(0, 0, 1);
        init.unshift({start: new Date(start).getTime() / 1000, end: new Date(end).getTime() / 1000, customers: 0, kept_customers: 0});
        end.setDate(end.getDate() - 1);
        end.setHours(23, 59, 59);
    }

    function mapper(data, period) {
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
        // Calcul de la présence/absence du user à ce moment
        var I = period.length - 1, res = null, label, features = [];
        if (period[I - 30].customerIsActive && data.dob) {
            label = period[I].customerIsActive ? -1 : 1;
            // compute time since creation of user
            var elapsed_days = (period[I].end - data.dob) / (3600 * 24 * 30);
            features.push(Math.round(elapsed_days));
            res = [label, features];
        }

        for (var i = 0; i < period.length; i++) 
            period[i].customerIsActive = undefined;
        return res;
    }    

    /* ------------------------------------------------------------------------------------ */
    // Write training set to disk (Workaround to write features file, bug on stream)
    /* ------------------------------------------------------------------------------------ */
    try {fs.readFileSync('tmp')}
    catch(e) {
        var cursor = db.collection('customers').find({}, {_id: 0}).limit(N - 1);
        var data = yield uc.objectStream(cursor, {N: N})
            .map(mapper, [init])
            .filter(function(data) {return data != null;})
            .collect();
        var str = '';
        for (var i in data)
            str += JSON.stringify(data[i]) + '\n';
        fs.writeFileSync('tmp', str);
    }

    /* ------------------------------------------------------------------------------------ */
    // Model training: Split the test set in 2 equal parts 0.7 and 0.3
    /* ------------------------------------------------------------------------------------ */
    var testSet = uc.textFile('/Users/cedricartigue/work/ugrid/demo/tmp')
        .map(function (line) {return JSON.parse(line)})
        .map(function (data) {return (Math.random() <= 0.3) ? [0, data] : [1, data];})

    var trainingSet = testSet.filter(function(data) {return (data[0] == 0)})
        .map(function (data) {return data[1]})
        .persist();

    var validationSet = testSet.filter(function(data) {return (data[0] == 1)})
        .map(function (data) {return data[1]});

    var nIterations = 1000, nFeatures = 1;
    var trainingSetLength = yield trainingSet.count();
    var model = new ugrid.ml.LogisticRegression(trainingSet, nFeatures, trainingSetLength);
    yield model.train(nIterations);

    /* ------------------------------------------------------------------------------------ */
    // Model fitting
    /* ------------------------------------------------------------------------------------ */
    var init = {weights: model.w, points: []}, n = 100, threshold_step = 1 / n;
    for (var i = 0; i < n; i++)
        init.points.push({threshold: i * threshold_step, tp: 0, fp: 0, tn: 0, fn: 0})

    function reducer(acc, data) {
        var w = acc.weights;
        var tmp = 0;
        for (var j = 0; j < data[1].length; j++)
            tmp += w[j] * data[1][j];
        var score = 1 / (1 + Math.exp(-tmp));

        for (var i = 0; i < acc.points.length; i++) {
            var pred = (score > acc.points[i].threshold) ? 1 : -1;
            if (data[0] == 1) {
                if (pred == 1) acc.points[i].tp++;
                else acc.points[i].fn++;
            } else {
                if (pred == 1) acc.points[i].fp++;
                else acc.points[i].tn++;
            }
        }
        return acc;
    }

    function combiner(a, b) {
        for (var i = 0; i < a.points.length; i++) {
            a.points[i].tn += b.points[i].tn;
            a.points[i].fn += b.points[i].fn;
            a.points[i].tp += b.points[i].tp;
            a.points[i].fp += b.points[i].fp;
        }
        return a;
    }

    var result = yield validationSet.aggregate(reducer, combiner, init);
    /* ------------------------------------------------------------------------------------ */
    // Metrics generation
    /* ------------------------------------------------------------------------------------ */
    var metrics = [], beta = 0.1, beta_squared = beta * beta;
    for (var i = 0; i < result.points.length; i++) {
        var ppv = result.points[i].tp / (result.points[i].tp + result.points[i].fp);
        var tpr = result.points[i].tp / (result.points[i].tp + result.points[i].fn);
        var fpr = result.points[i].fp / (result.points[i].fp + result.points[i].tn);
        var fmeasure = (1 + beta_squared) * ppv * tpr / (beta_squared * ppv + tpr);
        metrics.push({threshold: result.points[i].threshold, tpr: tpr, fpr: fpr, ppv: ppv, fmeasure: fmeasure});
    }

    console.log(result)

    console.log('threshold\tfpr\ttpr\tppv\tfmeasure')
    for (var i = 0; i < metrics.length; i++) {
        var thresh = String(metrics[i].threshold).replace('.', ',');
        var fpr = String(metrics[i].fpr).replace('.', ',');
        var tpr = String(metrics[i].tpr).replace('.', ',');
        var ppv = String(metrics[i].ppv).replace('.', ',');
        var fmeasure = String(metrics[i].fmeasure).replace('.', ',');
        console.log(thresh + '\t' + fpr + '\t' + tpr + '\t' + ppv + '\t' + fmeasure)
    }

    db.close();
    uc.end();
}).catch(ugrid.onError);
