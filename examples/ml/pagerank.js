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
    console.log(yield links.collect());

    // Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
    var ranks = links.map(function(link) {return [link[0], 1]});
    console.log(yield ranks.collect());

    // Calculates and updates URL ranks continuously using PageRank algorithm.
    for (var i = 0; i < iterations; i++) {
        var contribs = links.join(ranks).flatMap(function (entry) {
            // Calculates URL contributions to the rank of other URLs.
            var num_urls = entry[1][0].length;
            var contribs = [];
            for (var i = 0; i < num_urls; i++)
                contribs.push([entry[1][0][i], entry[1][1] / num_urls])
            return contribs;
        });
        console.log(yield contribs.collect());
        // ranks = contribs.reduceByKey(add).mapValues(function(e) return e[1] * 0.85 + 0.15);
    }

    // // Collects all URL ranks and dump them to console.
    // var res = yield ranks.collect();
    // for (var i = 0; i < res.length; i++)
    //     console.log(res[i][0] + ' has rank: ' + res[i][1]);

    uc.end();
}).catch(ugrid.onError);

