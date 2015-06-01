#!/usr/local/bin/node --harmony

// Chaque bloc ne prend qu'une input de type kv (keys -> reduce est indéfini)

var sources = ['parallelize', 'stream', 'textFile'];
var transfos = ['map', 'filter', 'flatMap', 'mapValues', 'sample', 'groupByKey', 'reduceByKey', 'distinct', 'flatMapValues', 'keys', 'values'];
var dualtransfo = ['rightOuterJoin', 'leftOuterJoin', 'intersection', 'subtract', 'crossProduct', 'coGroup', 'union', 'join'];
var actions = ['count', 'collect', 'reduce', 'lookup', 'countByValue',
	'countStream', 'collectStream', 'reduceStream', 'lookupStream', 'countByValueStream'];

var test_id = 0;
var dir = process.argv[2] ? (process.argv[2] + '/') : './';

function file () {
	var name = dir + 't' + (("000" + test_id++).slice(-4)) + '.js';
	return name + ' && chmod +x ' + name;
}

// Copy test files to tmp directory
console.log('cp test/automatic/kv.data /tmp/$USER; cp test/automatic/kv2.data /tmp/$USER');

// -------------------------------------------------------------------------------------------- //
// Single lineage
// -------------------------------------------------------------------------------------------- //
// source -> action
for (var i = 0; i < sources.length; i++)
	for (var j = 0; j < actions.length; j++) {
		console.log('./utils/b1.js ' + sources[i] + ' ' + actions[j] + ' > ' + file());
		if (sources[i] == 'stream') continue;	// do not mix persist with input stream
		console.log('./utils/b1.js ' + sources[i] + ' persist ' + actions[j] + ' > ' + file());
	}

// source -> transfo -> action
for (var i = 0; i < sources.length; i++)
	for (var j = 0; j < transfos.length; j++)
		for (var k = 0; k < actions.length; k++) {
			console.log('./utils/b1.js ' + sources[i] + ' ' + transfos[j] + ' ' + actions[k] + ' > ' + file());
			if (sources[i] == 'stream') continue;	// do not mix persist with input stream
			console.log('./utils/b1.js ' + sources[i] + ' persist ' + transfos[j] + ' ' + actions[k] + ' > ' + file());
			console.log('./utils/b1.js ' + sources[i] + ' ' + transfos[j] + ' persist ' + actions[k] + ' > ' + file());
		}

// Ajouter les tests des cas suivants
// source shuffleTransfo transfo action

// -------------------------------------------------------------------------------------------- //
// Dual lineage
// -------------------------------------------------------------------------------------------- //
// source source dualtransfo action
for (var i = 0; i < dualtransfo.length; i++)
	for (var j = 0; j < actions.length; j++) {
		// parallelize parallelize
		console.log('./utils/b1.js parallelize parallelize ' + dualtransfo[i] + ' ' + actions[j] + ' > ' + file());
		console.log('./utils/b1.js parallelize persist parallelize ' + dualtransfo[i] + ' ' + actions[j] + ' > ' + file());
		console.log('./utils/b1.js parallelize parallelize persist ' + dualtransfo[i] + ' ' + actions[j] + ' > ' + file());
		console.log('./utils/b1.js parallelize persist parallelize persist ' + dualtransfo[i] + ' ' + actions[j] + ' > ' + file());

		// parallelize textFile
		console.log('./utils/b1.js parallelize textFile ' + dualtransfo[i] + ' ' + actions[j] + ' > ' + file());
		console.log('./utils/b1.js parallelize persist textFile ' + dualtransfo[i] + ' ' + actions[j] + ' > ' + file());
		console.log('./utils/b1.js parallelize textFile persist ' + dualtransfo[i] + ' ' + actions[j] + ' > ' + file());
		console.log('./utils/b1.js parallelize persist textFile persist ' + dualtransfo[i] + ' ' + actions[j] + ' > ' + file());

		// parallelize stream
		console.log('./utils/b1.js parallelize stream ' + dualtransfo[i] + ' ' + actions[j] + ' > ' + file());
		console.log('./utils/b1.js parallelize persist stream ' + dualtransfo[i] + ' ' + actions[j] + ' > ' + file());
		//console.log('./utils/b1.js parallelize stream persist ' + dualtransfo[i] + ' ' + actions[j] + ' > ' + file());
		//console.log('./utils/b1.js parallelize persist stream persist ' + dualtransfo[i] + ' ' + actions[j] + ' > ' + file());

		// textFile parallelize
		console.log('./utils/b1.js textFile parallelize ' + dualtransfo[i] + ' ' + actions[j] + ' > ' + file());
		console.log('./utils/b1.js textFile persist parallelize ' + dualtransfo[i] + ' ' + actions[j] + ' > ' + file());
		console.log('./utils/b1.js textFile parallelize persist ' + dualtransfo[i] + ' ' + actions[j] + ' > ' + file());
		console.log('./utils/b1.js textFile persist parallelize persist ' + dualtransfo[i] + ' ' + actions[j] + ' > ' + file());

		// textFile textFile
		console.log('./utils/b1.js textFile textFile ' + dualtransfo[i] + ' ' + actions[j] + ' > ' + file());
		console.log('./utils/b1.js textFile persist textFile ' + dualtransfo[i] + ' ' + actions[j] + ' > ' + file());
		console.log('./utils/b1.js textFile textFile persist ' + dualtransfo[i] + ' ' + actions[j] + ' > ' + file());
		console.log('./utils/b1.js textFile persist textFile persist ' + dualtransfo[i] + ' ' + actions[j] + ' > ' + file());

		// textFile stream
		console.log('./utils/b1.js textFile stream ' + dualtransfo[i] + ' ' + actions[j] + ' > ' + file());
		console.log('./utils/b1.js textFile persist stream ' + dualtransfo[i] + ' ' + actions[j] + ' > ' + file());
		//console.log('./utils/b1.js textFile stream persist ' + dualtransfo[i] + ' ' + actions[j] + ' > ' + file());
		//console.log('./utils/b1.js textFile persist stream persist ' + dualtransfo[i] + ' ' + actions[j] + ' > ' + file());

		// stream parallelize
		console.log('./utils/b1.js stream parallelize ' + dualtransfo[i] + ' ' + actions[j] + ' > ' + file());
		//console.log('./utils/b1.js stream persist parallelize ' + dualtransfo[i] + ' ' + actions[j] + ' > ' + file());
		console.log('./utils/b1.js stream parallelize persist ' + dualtransfo[i] + ' ' + actions[j] + ' > ' + file());
		//console.log('./utils/b1.js stream persist parallelize persist ' + dualtransfo[i] + ' ' + actions[j] + ' > ' + file());

		// stream textFile
		console.log('./utils/b1.js stream textFile ' + dualtransfo[i] + ' ' + actions[j] + ' > ' + file());
		//console.log('./utils/b1.js stream persist textFile ' + dualtransfo[i] + ' ' + actions[j] + ' > ' + file());
		console.log('./utils/b1.js stream textFile persist ' + dualtransfo[i] + ' ' + actions[j] + ' > ' + file());
		//console.log('./utils/b1.js stream persist textFile persist ' + dualtransfo[i] + ' ' + actions[j] + ' > ' + file());

		// stream stream
		console.log('./utils/b1.js stream stream ' + dualtransfo[i] + ' ' + actions[j] + ' > ' + file());
		//console.log('./utils/b1.js stream persist stream ' + dualtransfo[i] + ' ' + actions[j] + ' > ' + file());
		//console.log('./utils/b1.js stream stream persist ' + dualtransfo[i] + ' ' + actions[j] + ' > ' + file());
		//console.log('./utils/b1.js stream persist stream persist ' + dualtransfo[i] + ' ' + actions[j] + ' > ' + file());
	}

process.stderr.write('\r\t\t\t\t\t\t\t\t\t\n\n');
