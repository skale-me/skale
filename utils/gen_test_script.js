#!/usr/local/bin/node --harmony

var co = require('co');
var fs = require('fs');
var ugrid = require('../');

// Chaque bloc ne prend qu'une input de type kv (keys -> reduce est indéfini)

var sources = ['parallelize', 'textFile'];
var transfos = ['map', 'filter', 'flatMap', 'mapValues', 'sample', 'groupByKey', 'reduceByKey', 'distinct', 'flatMapValues', 'keys', 'values'];
var dualtransfo = ['rightOuterJoin', 'leftOuterJoin', 'intersection', 'subtract', 'crossProduct', 'coGroup', 'union', 'join'];
var actions = ['count', 'collect', 'reduce', 'lookup', 'countByValue'];

var test_id = 0;
var dir = process.argv[2] ? (process.argv[2] + '/') : './';

function file () {
	var name = dir + 't' + (("000" + test_id++).slice(-4)) + '.js';
	return name + ' && chmod +x ' + name;
}

// Copy test files to tmp directory
console.log('cp test/automatic/kv.data /tmp; cp test/automatic/kv2.data /tmp');

// -------------------------------------------------------------------------------------------- //
// Single lineage
// -------------------------------------------------------------------------------------------- //
// source -> action
for (var i = 0; i < sources.length; i++)
	for (var j = 0; j < actions.length; j++)
		console.log('./utils/b1.js ' + sources[i] + ' ' + actions[j] + ' > ' + file())

// source -> persist -> action
for (var i = 0; i < sources.length; i++)
	for (var j = 0; j < actions.length; j++)
		console.log('./utils/b1.js ' + sources[i] + ' persist ' + actions[j] + ' > ' + file())

// source -> transfo -> action
for (var i = 0; i < sources.length; i++)
	for (var j = 0; j < transfos.length; j++)
		for (var k = 0; k < actions.length; k++) {
			if ((transfos[j] == 'keys') && (actions[k] == 'reduce')) continue;
			if ((transfos[j] == 'values') && (actions[k] == 'reduce')) continue;
			console.log('./utils/b1.js ' + sources[i] + ' ' + transfos[j] + ' ' + actions[k] + ' > ' + file())
		}
// source -> persist -> transfo -> action
for (var i = 0; i < sources.length; i++)
	for (var j = 0; j < transfos.length; j++)
		for (var k = 0; k < actions.length; k++) {
			if ((transfos[j] == 'keys') && (actions[k] == 'reduce')) continue;
			if ((transfos[j] == 'values') && (actions[k] == 'reduce')) continue;
			console.log('./utils/b1.js ' + sources[i] + ' persist ' + transfos[j] + ' ' + actions[k] + ' > ' + file())
		}
// source -> transfo -> persist -> action
for (var i = 0; i < sources.length; i++)
	for (var j = 0; j < transfos.length; j++)
		for (var k = 0; k < actions.length; k++) {
			if ((transfos[j] == 'keys') && (actions[k] == 'reduce')) continue;
			if ((transfos[j] == 'values') && (actions[k] == 'reduce')) continue;
			console.log('./utils/b1.js ' + sources[i] + ' ' + transfos[j] + ' persist ' + actions[k] + ' > ' + file());
		}
// Ajouter les tests des cas suivants
// source shuffleTransfo transfo action

// -------------------------------------------------------------------------------------------- //
// Dual lineage
// -------------------------------------------------------------------------------------------- //
// parallelize parallelize dualtransfo action
for (var i = 0; i < dualtransfo.length; i++)
	for (var j = 0; j < actions.length; j++)
		console.log('./utils/b1.js parallelize parallelize ' + dualtransfo[i] + ' ' + actions[j] + ' > ' + file())

// parallelize persist parallelize dualtransfo action
for (var i = 0; i < dualtransfo.length; i++)
	for (var j = 0; j < actions.length; j++)
		console.log('./utils/b1.js parallelize persist parallelize ' + dualtransfo[i] + ' ' + actions[j] + ' > ' + file())

// parallelize parallelize persist dualtransfo action
for (var i = 0; i < dualtransfo.length; i++)
	for (var j = 0; j < actions.length; j++)
		console.log('./utils/b1.js parallelize parallelize persist ' + dualtransfo[i] + ' ' + actions[j] + ' > ' + file())

// parallelize persist parallelize persist dualtransfo action
for (var i = 0; i < dualtransfo.length; i++)
	for (var j = 0; j < actions.length; j++)
		console.log('./utils/b1.js parallelize persist parallelize persist ' + dualtransfo[i] + ' ' + actions[j] + ' > ' + file())



// TODO
// console.log('./b1.js parallelize parallelize union count > tmp.js && node --harmony tmp.js; sleep 1');
// console.log('./b1.js parallelize textFile union count > tmp.js && node --harmony tmp.js; sleep 1');
// console.log('./b1.js textFile parallelize union count > tmp.js && node --harmony tmp.js; sleep 1');
// console.log('./b1.js textFile textFile union count > tmp.js && node --harmony tmp.js; sleep 1');




