#!/usr/local/bin/node --harmony
'use strict';

var sizeOf = require('./sizeof.js');

var a = [['hello', 1], ['world', 2]];

console.log(sizeOf(a))