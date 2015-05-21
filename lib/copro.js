'use strict';

var vm = require('vm');
var co = require('co');
var ugrid = require('../');
var trace = require('line-trace');
var esprima = require('./esprima.js');
var escodegen = require('escodegen');

var webid = process.env.UGRID_WEBID;
var uc;

var sandbox = {
	co: co,
	uc: uc,
	ugrid: ugrid,
	webid: webid,
	trace: trace,
	console: console,
	process: process
};

var initCode = 
	'function plot(data) {' +
		'uc.send(0, {cmd: "plot", id: webid, data: data});' +
	'}';
	// +
	// 'co(function *() {' +
	// 	'uc = yield ugrid.context({noworker: true});' +
	// '}).catch(ugrid.onError);';

// Transform an AST with variable(s) declaration(s) into
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
// This permits to use yield statements in script snippets

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
				transform(o.handlers[j].body);
			transform(o.handler.body);
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

co(function *() {
	uc = yield ugrid.context({noworker: true});
	sandbox.uc = uc;
	vm.createContext(sandbox);
	vm.runInContext(initCode, sandbox);
	process.stdin.setEncoding('utf8');
	process.stdin.on('data', function(data) {
		var ast = esprima.parse(data, {tolerant: true});
		transform(ast.body);
		var src = escodegen.generate(ast);
		src = 'co(function *() {' + src + '}).catch(ugrid.onError);';
		vm.runInContext(src, sandbox);
	});
}).catch(ugrid.onError);
