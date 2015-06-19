#!/usr/local/bin/node --harmony

var fs = require('fs');
var Lines = require('../lib/lines.js');

// Load in memory dataset config file
var config = JSON.parse(fs.readFileSync('config.json', {encoding: 'utf8'}));
console.log('# Dataset configuration')
console.log(JSON.stringify(config, null, 4));

function readPartition(config, callback, idx) {
	var nParts = config.parts.length;
	idx = idx || 0;
	var stream = fs.createReadStream(config.parts[idx].tail).pipe(new Lines());

	console.log('\nReading partition n° ' + config.parts[idx].index);

	stream.on('data', function(line) {
		var data = config.parts[idx].key ? [config.parts[idx].key, line] : line;
		console.log(data)
	});

	stream.on('end', function() {
		if (++idx < config.parts.length) readPartition(config, callback, idx);
		else callback && callback();
	});
}

readPartition(config);
