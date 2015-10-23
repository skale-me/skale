#!/usr/local/bin/node --harmony
'use strict';

// groupByKey
var ugrid = require('ugrid');

var uc = ugrid.context();

var data = [{one:'a'}, {one:'b'}, {two:'c'}, {two:'d'}, {two:'e'}, {three:'f'}];
var vector = uc.parallelize(data);
vector.groupByKey().collect(function(err,res){
	   console.log(res)
	   uc.end();
});
