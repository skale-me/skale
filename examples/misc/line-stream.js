#!/usr/bin/env node

var fs = require('fs');
var uc = require('ugrid').context();

//var s = uc.lineStream(fs.createReadStream('/etc/hosts', 'utf8'));
var s = uc.objectStream(fs.createReadStream('/etc/hosts', 'utf8'));

s.collect().on('data', console.log)

var s2 = uc.lineStream(fs.createReadStream('/etc/hosts', 'utf8'));

s2.collect().on('data', console.log)
