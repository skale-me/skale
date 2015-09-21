#!/usr/local/bin/node --harmony
'use strict';

var p = 0.2;
var l = 10;
var L = l / (1 - p);
var N = 100;

var points = [];

for (var i = 0; i < N; i++)
    points.push(Math.random() * L)

var sub = points.filter(function(val) {return val < l});

var p_hat = sub.length / N;

// console.log(sub)
// console.log(points)

console.log('p = ' + (1 - p));
console.log('p_hat = ' + p_hat);
