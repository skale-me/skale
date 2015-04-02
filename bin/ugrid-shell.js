#!/usr/local/bin/node --harmony

'use strict';

var repl = require('repl');
var co = require('co');
var thenify = require('thenify');
var ugrid = require('../');

function corepl(repl) {
	var prevEval = repl.eval;

	function coeval(cmd, context, filename, callback) {
		if (cmd.match(/\W*yield\s+/))
			cmd = "co(function *() {" + cmd.replace(/^\s*var\s+/, "") + "});"
		prevEval.call(repl, cmd, context, filename, function (err, res) {
			if (err || !res || typeof res.then != 'function')
				return callback(err, res);
			res.then(function(val) {callback(null, val)}, callback);
		});
	}

	repl.eval = coeval;
	return repl;
}

var context = corepl(repl.start({
	prompt: 'ugrid> '
})).context;

context.co = co;
context.ugrid = ugrid;

// For testing
context.sleep = thenify.withCallback(function (delay, callback) {
    setTimeout(callback, delay);
});
context.badfun = thenify(function(callback) {
	throw new Error("bad fun");
});
