#!/usr/local/bin/node --harmony

'use strict';

var fs = require('fs');
var vm = require('vm');
var co = require('co');
var ugrid = require('../');
var trace = require('line-trace');
var esprima = require('./esprima.js');
var escodegen = require('escodegen');

var webid = process.env.UGRID_WEBID;
var uc;
var input = process.stdin, first = true, fixcode = true;

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
		'uc.send(0, {cmd: "plot", id: webid, data: data, file: uc._file});' +
		//'uc.send(0, {cmd: "plot", id: webid, data: data});' +
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
	for (var i = 0; i < node.length; i++) {
		var o = node[i];
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
			for (var j = 0; j < o.handlers.length; j++)
				transform(o.handlers[j].body.body);
			transform(o.handler.body.body);
			transform(o.finalizer.body);
			break;
		case 'SwitchStatement':
			for (var j = 0; j < o.cases.length; j++)
				transform(o.cases[j].consequent);
			break;
		case 'ForStatement':
			if (o.init.type === 'VariableDeclaration') {
				var res = var2assign(o.init);
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
	input = fs.createReadStream(process.argv[2]);
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
			msg = JSON.parse(msg);
			data = msg.data;
			uc._file = msg.file;
		} else data = msg;
		if (first) {
			data = data.replace(/^(#.*\n)*(exec .*\n)?/, '');
			first = false;
		}
		if (fixcode) {
			var ast = esprima.parse(data, {tolerant: true});
			transform(ast.body);
			data = escodegen.generate(ast);
		}
		data = 'co(function *() {' + data + '}).catch(ugrid.onError);';
		vm.runInContext(data, sandbox);
	});
}).catch(ugrid.onError);
