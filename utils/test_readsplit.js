#!/usr/local/bin/node

var splitLocalFile = require('./readSplit.js').splitLocalFile;
var splitHDFSFile = require('./readSplit.js').splitHDFSFile;
var readSplit = require('./readSplit.js').readSplit;

// splitLocalFile('./split.dat', process.argv[2] || 1, function(split) {
// 	var s = 0, cnt = 0;

// 	for (var i in split)
// 		console.log(JSON.stringify(split[i]));

// 	function processLine(line) {
// 		console.log('# line %d : %s', cnt++, line);
// 	};

// 	function nextSplit() {
// 		if (++s >= split.length) {console.log('===========> Number of lines = ' + cnt)}
// 		else readSplit(split, s, processLine, nextSplit)
// 	}

// 	readSplit(split, s, processLine, nextSplit);
// });

/*
	starter le dfs: /usr/local/Cellar/hadoop/2.6.0/sbin/start-dfs.sh
	good url is hdfs://localhost:9000/v, but use only /v for now
*/

splitHDFSFile('/svm200MB', process.argv[2] || 4, function(split) {
	console.log('\n############# SPLIT')
	for (var i in split) 
		console.log(JSON.stringify(split[i]))
	
	// console.log(split);
	// var s = 0, cnt = 0;

	// for (var i in split)
	// 	console.log(JSON.stringify(split[i]));

	// function processLine(line) {
	// 	console.log('# line %d : %s', cnt++, line);
	// };

	// function nextSplit() {
	// 	if (++s >= split.length) {console.log('===========> Number of lines = ' + cnt)}
	// 	else readSplit(split, s, processLine, nextSplit)
	// }

	// readSplit(split, s, processLine, nextSplit);
});