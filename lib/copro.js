#!/usr/local/bin/node --harmony

'use strict';

var fs = require('fs');
var vm = require('vm');
var co = require('co');
var ugrid = require('../');
var Lines = require('./lines.js');
var trace = require('line-trace');
var esprima = require('esprima');
var escodegen = require('escodegen');

var webid = process.env.UGRID_WEBID;
var dest = process.env.UGRID_DEST;
var uc;
var input = process.stdin, first = true, fixcode = true;

trace.setTracer(console.error);

var sandbox = {
	co: co,
	ugrid: ugrid,
	webid: webid,
	trace: trace,
	console: console,
	process: process
};

var initCode = 'function plot() {}';

if (webid) {
	initCode = 'function plot(data) {' +
		'uc.send(0, {cmd: "plot-' + webid + '", id: "' + dest + '", data: data, file: uc._file});' +
	'}';
	console.log = function (d) {process.stdout.write(JSON.stringify({data: d, file: uc._file}) + '\n');};
}

// Transform an AST node of variable(s) declaration(s) into
// a sequence of assignements.

function var2assign(o) {
	var seq = [];
	for (var j = 0; j < o.declarations.length; j++) {
		seq.push({
			type: 'AssignmentExpression',
			operator: '=',
			left: o.declarations[j].id,
			right: o.declarations[j].init ?
				o.declarations[j].init :
				{type: 'Identifier', name: 'undefined'}
		});
	}
	return seq;
}

// Remove 'var' keyword in all statements of the current scope,
// forcing variables to be defined in the global scope.
// This allows to wrap snippets in 'co(function *() { <snippet> });'
// to support 'yield' statements, and allow access to variables declared
// in a different snippet.

function transform(node) {
	var i, j, o, res;

	for (i = 0; i < node.length; i++) {
		o = node[i];
		switch (o.type) {
		case 'VariableDeclaration':
			node[i] = {
				type: 'ExpressionStatement',
				expression: {type: 'SequenceExpression', expressions: var2assign(o)}
			};
			break;
		case 'IfStatement':
			transform(o.consequent.body);
			transform(o.alternate.body);
			break;
		case 'WhileStatement':
		case 'DoWhileStatement':
			transform(o.body.body);
			break;
		case 'TryStatement':
			transform(o.block.body);
			for (j = 0; j < o.handlers.length; j++)
				transform(o.handlers[j].body.body);
			transform(o.handler.body.body);
			transform(o.finalizer.body);
			break;
		case 'SwitchStatement':
			for (j = 0; j < o.cases.length; j++)
				transform(o.cases[j].consequent);
			break;
		case 'ForStatement':
			if (o.init.type === 'VariableDeclaration') {
				res = var2assign(o.init);
				if (res.length == 1) o.init = res[0];
				else o.init = {type: 'SequenceExpression', expressions: res};
			}
			if (o.body.body) transform(o.body.body);
			break;
		}
	}
}

if (process.argv.length > 2) {
	fixcode = false;
	input = fs.createReadStream(process.argv[2]).pipe(new Lines());
}

co(function *() {
	uc = yield ugrid.context();
	sandbox.uc = uc;
	vm.createContext(sandbox);
	vm.runInContext(initCode, sandbox);
	input.setEncoding('utf8');
	input.on('data', function(msg) {
		var data;
		if (webid) {
			try {
				msg = JSON.parse(msg);
			} catch (err) {
				console.error('JSON.parse failed on: ' + msg);
				return;
			}
			data = msg.data;
			uc._file = msg.file;
		} else data = msg;
		if (first) {
			data = data.replace(/^(#.*\n)*(exec .*\n)?/, '');
			first = false;
		}
		if (fixcode) {
			trace(data)
			var ast = esprima.parse(data, {tolerant: true});
			transform(ast.body);
			data = escodegen.generate(ast);
		}
		console.error(data);
		data = 'co(function *() {' + data + '}).catch(ugrid.onError);';
		vm.runInContext(data, sandbox);
	});
}).catch(ugrid.onError);
