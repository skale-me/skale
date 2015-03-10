#!/usr/bin/env node

// Todo:
// - record/replay input messages
// - handle foreign messages
// - authentication (register)
// - statistics in monitoring
// - topics permissions (who can publish / subscribe)

'use strict';

var net = require('net');
var util = require('util');
var stream = require('stream');
var uuidGen = require('node-uuid');
var UgridClient = require('../lib/ugrid-client.js');
var webSocketServer = require('ws').Server;
var websocket = require('websocket-stream');

var opt = require('node-getopt').create([
	['h', 'help', 'print this help text'],
	['H', 'Host=ARG', 'primary server host (default none)'],
	['n', 'name=ARG', 'advertised server name (default localhost)'],
	['P', 'Port=ARG', 'primary server port (default none)'],
	['p', 'port=ARG', 'server port (default 12346)'],
	['w', 'wsport=ARG', 'listen on websocket port (default none)'],
	['v', 'version', 'print version']
]).bindHelp().parseSystem();

var clients = {};
var clientNum = 1;
var clientMax = UgridClient.minMulticast;
var minMulticast = UgridClient.minMulticast;
var topics = {};
var topicNum = -1;
var UInt32Max = 4294967296;
var topicMax = UInt32Max - minMulticast;
var topicIndex = {};
//var name = opt.options.name || 'localhost';		// Unused until FT comes back
var port = opt.options.port || 12346;
var msgCount = 0;
var wss;
var wsport = opt.options.wsport || port + 2;
var crossbar = {};

function SwitchBoard(sock) {
	if (!(this instanceof SwitchBoard))
		return new SwitchBoard(sock);
	stream.Transform.call(this, {objectMode: true});
	sock.index = getClientNumber();
	crossbar[sock.index] = sock;
	this.sock = sock;
}
util.inherits(SwitchBoard, stream.Transform);

SwitchBoard.prototype._transform = function (chunk, encoding, done) {
	var o = {}, to = chunk.readUInt32LE(0, true);
	if (to >= minMulticast)	{	// Multicast
		var sub = topics[to - minMulticast].sub, len = sub.length, n = 0;
		if (len == 0) return done();
		for (var i in sub) {
			// Flow control: adjust to the slowest receiver
			if (crossbar[sub[i]]) {
				crossbar[sub[i]].write(chunk, function () {
					if (++n == len) done();
				});
			} else if (--len == 0) done();
		}
	} else if (to > 1) {	// Unicast
		if (crossbar[to]) crossbar[to].write(chunk, done);
		else done();
	} else if (to == 1) {	// Foreign (to be done)
	} else if (to == 0) {	// Server request
		try {
			o = JSON.parse(chunk.slice(8));
		} catch (error) {
			console.error(error);
			return done();
		}
		if (!(o.cmd in clientRequest)) {
			o.error = 'Invalid command: ' + o.cmd;
			o.cmd = 'reply';
			this.sock.write(UgridClient.encode(o), done);
		} else if (clientRequest[o.cmd](this.sock, o)) {
			o.cmd = 'reply';
			this.sock.write(UgridClient.encode(o), done);
		} else done();
	}
};

// Client requests functions, return true if a response must be sent
// to client, false otherwise. Reply data, if any,  must be set in msg.data.
var clientRequest = {
	connect: function (sock, msg) {
		register(null, msg, sock);
		return true;
	},
	end: function (sock, msg) {
		sock.client.end = true;
		return false;
	},
	devices: function (sock, msg) {
		msg.data = devices(msg.data);
		return true;
	},
	get: function (sock, msg) {
		msg.data = clients[msg.data] ? clients[msg.data].data : null;
		return true;
	},
	set: function (sock, msg) {
		if (typeof msg.data != 'object') return false;
		for (var i in msg.data)
			sock.client.data[i] = msg.data[i];
		pubmon({event: 'set', uuid: sock.client.uuid, data: msg.data});
		return false;
	},
	id: function (sock, msg) {
		msg.data = msg.data in clients ? clients[msg.data].index : null;
		return true;
	},
	tid: function (sock, msg) {
		var topic = msg.data;
		var n = msg.data = getTopicId(topic);
		// First to publish becomes topic owner
		if (!topics[n].owner) {
			topics[n].owner = sock.client;
			sock.client.topics[n] = true;
		}
		return true;
	},
	subscribe: function (sock, msg) {
		subscribe(sock.client, msg.data);
		return false;
	},
	unsubscribe: function (sock, msg) {
		unsubscribe(sock.client, msg.data);
		return false;
	}
};

// Create a source stream and topic for monitoring info publishing
var mstream = new SwitchBoard({});
var monid =  getTopicId('monitoring') + minMulticast;
function pubmon(data) {
	mstream.write(UgridClient.encode({cmd: 'monitoring', id: monid, data: data}));
}

console.log("## Started " + Date());
// Start a TCP server
if (port) {
	net.createServer(handleConnect).listen(port);
	console.log("## Listening TCP on " + port);
}

// Start a websocket server if a listening port is specified on command line
if (wsport) {
	console.log("## Listening WebSocket on " + wsport);
	wss = new webSocketServer({port: wsport});
	wss.on('connection', function (ws) {
		var sock = websocket(ws);
		sock.ws = true;
		handleConnect(sock);
		// Catch error/close at websocket level in addition to stream level
		ws.on('close', function () {
			handleClose(sock);
		});
		ws.on('error', function () {
			console.log('## websocket connection error');
			console.log(error);
		});
	});
}

function handleClose(sock) {
	console.log('## connection closed');
	var cli = sock.client;
	if (cli) {
		pubmon({event: 'disconnect', uuid: cli.uuid});
		cli.sock = null;
	}
	if (sock.index) delete crossbar[sock.index];
	for (var i in cli.topics) {		// remove owned topics
		delete topicIndex[topics[i].name];
		delete topics[i];
	}
	if (cli.end) delete clients[cli.uuid];
	sock.removeAllListeners();
}

function handleConnect(sock) {
	if (sock.ws) { 
		console.log('Connect websocket from ' + sock.socket.upgradeReq.headers.origin);
	} else {
		console.log('Connect tcp ' + sock.remoteAddress + ' ' + sock.remotePort);
		sock.setNoDelay();
	}
	sock.pipe(new UgridClient.FromGrid()).pipe(new SwitchBoard(sock));
	sock.on('end', function () {
		handleClose(sock);
	});
	sock.on('error', function (error) {
		console.log('## connection error');
		console.log(error);
	});
}

function getClientNumber() {
	var n = 100000;
	do {
		clientNum = (clientNum < clientMax) ? clientNum + 1 : 2;
	} while (clientNum in crossbar && --n);
	if (!n) throw "getClientNumber failed"
	return clientNum;
}

function register(from, msg, sock)
{
	var uuid = msg.uuid || uuidGen.v1();
	sock.client = clients[uuid] = {
		index: sock.index,
		uuid: uuid,
		owner: from ? from : uuid,
		data: msg.data || {},
		sock: sock,
		topics: {}
	};
	pubmon({event: 'connect', uuid: uuid, data: msg.data});
	msg.data = {uuid: uuid, token: 0, id: sock.index};
}

function devices(query) {
	var result = [];
	for (var i in clients) {
		if (!clients[i].sock) continue;
		var match = true;
		for (var j in query)
			if (!clients[i].data || clients[i].data[j] != query[j]) {
				match = false;
				break;
			}
		if (match)
			result.push({
				uuid: i,
				id: clients[i].index,
				ip: clients[i].sock.remoteAddress,
				data: clients[i].data
			});
	}
	return result;
}

function getTopicId(topic) {
	if (topic in topicIndex) return topicIndex[topic];
	var n = 10000;
	do {
		topicNum = (topicNum < topicMax) ? topicNum + 1 : 0;
	} while (topicNum in topics && --n);
	if (!n) throw "getTopicId failed";
	topics[topicNum] = {name: topic, id: topicNum, sub: []};
	topicIndex[topic] = topicNum;
	return topicIndex[topic]
}

function subscribe(client, topic) {
	var sub = topics[getTopicId(topic)].sub
	if (sub.indexOf(client.index) < 0)
		sub.push(client.index);
}

function unsubscribe(client, topic) {
	if (!(topic in topicIndex)) return;
	var sub = topics[topicIndex[topic]].sub, i = sub.indexOf(client.index);
	if (i >= 0) sub.splice(i, 1);
}
