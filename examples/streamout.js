#!/usr/local/bin/node --harmony
'use strict';

var ugrid = require('ugrid');

ugrid.context(function(err, uc) {
    if (err) {console.log(err); process.exit();}
    console.log('# Connected to ugrid');
    var data = [['one', 'a'], ['one', 'b'], ['two', 'c'], ['two', 'd'], ['two', 'e'], ['three', 'f']];

    var out = uc.parallelize(data).collect();

    out.on('data', function(data) {
    	console.log('New element');
    	console.log(data);
    })

    out.on('end', uc.end);

 //    var out = uc.parallelize(data).stream({stream: true})
 //    out.pipe(process.stdout);
	// out.on('end', uc.end);
});

// lookup
// var ugrid = require('ugrid');

// ugrid.context(function(err, uc) {
//     if (err) {console.log(err); process.exit();}
//     console.log('# Connected to ugrid');
//     var data = [['one', 'a'], ['one', 'b'], ['two', 'c'], ['two', 'd'], ['two', 'e'], ['three', 'f']];

//     var out = uc.parallelize(data).lookup('one');
//     out.on('data', function(data) {
//     	console.log(data);
//     })
//     out.on('end', uc.end);
// });

// count by value
// var ugrid = require('ugrid');

// ugrid.context(function(err, uc) {
//     if (err) {console.log(err); process.exit();}
//     console.log('# Connected to ugrid');
//     var data = [['one', 'a'], ['one', 'b'], ['two', 'c'], ['two', 'd'], ['two', 'e'], ['three', 'f']];

//     uc.parallelize(data).countByValue().on('data', function(data) {
//     	console.log(data);
//     }).on('end', uc.end);
// });

// countByKey
// var ugrid = require('ugrid');

// ugrid.context(function(err, uc) {
//     if (err) {console.log(err); process.exit();}
//     console.log('# Connected to ugrid');
//     var data = [['one', 'a'], ['one', 'b'], ['two', 'c'], ['two', 'd'], ['two', 'e'], ['three', 'f']];

//     uc.parallelize(data).countByKey().on('data', function(data) {
//     	console.log(data);
//     }).on('end', uc.end);
// });