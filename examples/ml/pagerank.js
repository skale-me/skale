#!/usr/local/bin/node --harmony
'use strict';

var co = require('co');
var ugrid = require('../..');

var opt = require('node-getopt').create([
    ['h', 'help', 'print this help text'],
    ['f', 'F=ARG', 'URL input file'],
    ['i', 'I=ARG', 'number of iterations']
]).bindHelp().parseSystem();

var file = opt.options.F;
var iterations = Number(opt.options.I) || 1;

// links = textFile map distinct groupByKey persist
// ranks = links map
// LOOP
//     contribs = links join ranks flatMap
//     ranks = contribs reduceByKey mapValues
// --> 3eme job: contribs = groupByKey join groupByKey map flatMap

co(function *() {
    var uc = yield ugrid.context();

    // Loads all URLs from input file and initialize their neighbors.
    var links = uc.textFile(file)
        .map(function(line) {return line.split(' ')})   // Parses a urls pair string into urls pair
        .distinct()
        .groupByKey()
        .persist();
    // console.log('Links');
    // console.log(yield links.collect());

    // Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
    var ranks = links.map(function(link) {return [link[0], 1]});
    // console.log('\nRanks')
    // console.log(yield ranks.collect());

    // Calculates and updates URL ranks continuously using PageRank algorithm.
    for (var i = 0; i < iterations; i++) {
        // var tmp = yield links.join(ranks).collect();
        // console.log('\nJoin');
        // console.log(tmp[0]);

        // var contribs = links.join(ranks).flatMap(function (entry) {
        //     // Calculates URL contributions to the rank of other URLs.
        //     console.log('\nNew flatMap entry')
        //     console.log(entry);
        //     var num_urls = entry[1][0].length;
        //     var contribs = [];
        //     for (var i = 0; i < num_urls; i++)
        //         contribs.push([entry[1][0][i], entry[1][1] / num_urls]);
        //     console.log(contribs)
        //     return contribs;
        // });
        // // console.log('\nContribs')
        // // console.log(yield contribs.collect());
        // ranks = contribs.reduceByKey(function(a, b) {
        //     console.log('\nNew RBK entry')
        //     console.log(a);
        //     console.log('init')
        //     console.log(b);
        //     a += b;
        //     console.log('after = ' + a)
        //     return a;
        // }, 0).mapValues(function(e) {return e[1] * 0.85 + 0.15});

        function computeContribs(entry) {
            // Calculates URL contributions to the rank of other URLs.
            var num_urls = entry[1][0].length;
            var contribs = [];
            for (var i = 0; i < num_urls; i++)
                contribs.push([entry[1][0][i], entry[1][1] / num_urls]);
            return contribs;
        }

        function sumContribs(a, b) {return a + b;}

        ranks = links.join(ranks)
            .flatMap(computeContribs)
            .reduceByKey(sumContribs, 0)
            .mapValues(function(e) {return e * 0.85 + 0.15});
    }

    // // Collects all URL ranks and dump them to console.
    var res = yield ranks.collect();
    console.log('\n Updated ranks')
    console.log(res);
    // for (var i = 0; i < res.length; i++)
    //     console.log(res[i][0] + ' has rank: ' + res[i][1]);

    uc.end();
}).catch(ugrid.onError);

