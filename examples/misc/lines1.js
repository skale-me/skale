#!/usr/bin/env node

// Say if last char of stream is a newline or not

var lines = require('../../lib/lines.js')();
process.stdin.pipe(lines);
lines.on('endNewline', function (isNewline) {
	console.log("End newline: " + isNewline);
});
