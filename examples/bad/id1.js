#!/usr/local/bin/node --harmony
'use strict';

// groupByKey
var ugrid = require('ugrid');

ugrid.context(function(err, uc) {
   if (err) {console.log(err); process.exit();}
   console.log('# Connected to ugrid');
   var data = [{one:'a'}, {one:'b'}, {two:'c'}, {two:'d'}, {two:'e'}, {three:'f'}];
   var vector = uc.parallelize(data);
   vector.groupByKey().collect(function(err,res){
           console.log(res)
           uc.end();
   })
});
