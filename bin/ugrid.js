#!/usr/local/bin/node

'use strict';

var net = require('net');
var util = require('util');
var stream = require('stream');
var uuidGen = require('node-uuid');
var ugridMsg = require('../lib/ugrid-msg.js');
var UgridClient = require('../lib/ugrid-client.js');
var webSocketServer = require('ws').Server;
var websocket = require('websocket-stream');

var opt = require('node-getopt').create([
	['h', 'help', 'print this help text'],
	['H', 'Host=ARG', 'primary server host (default none)'],
	['n', 'name=ARG', 'advertised server name (default localhost)'],
	['P', 'Port=ARG', 'primary server port (default none)'],
	['p', 'port=ARG', 'server port (default 12346)'],
	['s', 'statistics', 'print periodic statistics'],
	['w', 'wsport=ARG', 'listen on websocket port (default none)'],
	['v', 'version', 'print version']
]).bindHelp().parseSystem();

var clients = {};
//var tsocks = [null, null, null, null];
var clientMax = 4;
var name = opt.options.name || 'localhost';
var port = opt.options.port || 12346;
var primary = {host: opt.options.Host, port: opt.options.Port || 12346};
var secondary;
var msgCount = 0;
var cli;
var wss;
var crossbar = [], crossn = 4;

function SwitchBoard(sock) {
	if (!(this instanceof SwitchBoard))
		return new SwitchBoard(sock);
	stream.Transform.call(this, {objectMode: true});
	this.crossIndex = sock.crossIndex = crossn++;
	crossbar[this.crossIndex] = sock;
	this.sock = sock;
}
util.inherits(SwitchBoard, stream.Transform);

SwitchBoard.prototype._transform = function (chunk, encoding, done) {
	var i, o = {}, to = chunk.readInt32LE(0, true);
	if (to > 3) {			// Unicast
		if (crossbar[to]) crossbar[to].write(chunk, done);
		else done();
	} else if (to == 3) {	// Foreign
	} else if (to == 2) {	// Multicast
	} else if (to == 1) {	// Broadcast
	} else if (to == 0) {	// Server request
		try {
			o = JSON.parse(chunk.slice(8));
			if (!(o.cmd in clientCommand)) throw 'Invalid command: ' + o.cmd;
			o.data = clientCommand[o.cmd](this.sock, o);
		} catch (error) {
			o.error = error;
			console.error(error);
		}
		o.cmd = 'reply';
		this.sock.write(ugridMsg.encode(o), done);
	}
};

var clientCommand = {
	connect: function (sock, msg) {
		return register(null, msg, sock);
	},
	devices: function (sock, msg) {
		return devices(msg.data);
	},
	get: function (sock, msg) {
		return clients[msg.data] ? clients[msg.data].data : 'error: not found';
	}
};

// Start a websocket server if a listening port is specified on command line
if (opt.options.wsport) {
	wss = new webSocketServer({port: opt.options.wsport});
	wss.on('connection', function (ws) {
		console.log('websocket connect');
		var sock = handleConnect(websocket(ws));
		ws.on('close', function () {});
	});
}

// Start a TCP server
net.createServer(handleConnect).listen(port);
console.log("## Started " + Date());

function handleConnect(sock) {
	sock.setNoDelay();
	sock.pipe(ugridMsg.Decoder()).pipe(SwitchBoard(sock));
	sock.on('end', function () {
		if (sock.crossIndex) try {delete crossbar[sock.crossIndex];} catch (e) {};
		console.log('## connection end');
	});
	sock.on('error', function (error) {
		console.log('## connection error');
		console.log(error);
		console.log(sock);
	});
}

function handleClose(sock) {
	var client = sock.client;
	//if (!client || !client.sock) return;
	console.log('Disconnect ' + client.data.type + ' ' + client.index + ': ' + client.uuid);
	//client.sock = tsocks[client.index] = null;
}

function register(from, msg, sock)
{
	var uuid = msg.uuid || uuidGen.v1(), index = clientMax++;
	// sock.client = clients[uuid] = {
	clients[uuid] = {
		index: index,
		uuid: uuid,
		owner: from ? from : uuid,
		data: msg.data || {},
		sock: sock,
		subscribers: []
	};
	//tsocks[index] = sock;
	return {uuid: uuid, token: 0, id: index};
}

function devices(query) {
	var result = [];
	for (var i in clients) {
		if (!clients[i].sock.remoteAddress) continue;
		//if (!tsocks[clients[i].index]) continue;
		var match = true;
		for (var j in query)
			if (!clients[i].data || clients[i].data[j] != query[j]) {
				match = false;
				break;
			}
		if (match)
			result.push({uuid: i, id: clients[i].index, ip: clients[i].sock.remoteAddress});
	}
	return result;
}

if (opt.options.statistics) {
	setInterval(function () {
		console.log('msg: ' + (msgCount / 5) + ' msg/s');
		msgCount = 0;
	}, 10000);
}
