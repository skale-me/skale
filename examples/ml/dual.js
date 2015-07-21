#!/usr/local/bin/node --harmony
'use strict';

var co = require('co');
var ugrid = require('../..');

// Example with graph cycles
co(function *() {
    var uc = yield ugrid.context();

	// var v1 = [['www.binge.fr', ['wwww.google.com']], ['www.yahoo.fr', ['wwww.google.com']], ['www.google.fr', ['wwww.binge.fr']]];
	var v1 = [['www.binge.fr', 1], ['www.yahoo.fr', 1], ['www.google.fr', 1]];	
	var v2 = [['www.binge.fr', 1], ['www.yahoo.fr', 1], ['www.google.fr', 1]];

    var links = uc.parallelize(v1);
    var ranks = uc.parallelize(v2);

	ranks = links.join(ranks).mapValues(function(a) {return a[0] + a[1]});
	// console.log('iteration 1')
 //    var res = yield ranks.collect();
 //    for (var i in res) console.log(res[i]);

	ranks = links.join(ranks).mapValues(function(a) {return a[0] + a[1]});
	ranks = links.join(ranks).mapValues(function(a) {return a[0] + a[1]});	
	ranks = links.join(ranks).mapValues(function(a) {return a[0] + a[1]});	
	// console.log('iteration 2')
    var res = yield ranks.collect();
    for (var i in res) console.log(res[i]);

    uc.end();
}).catch(ugrid.onError);
