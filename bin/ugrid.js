#!/usr/local/bin/node

'use strict';

var net = require('net');
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
var tsocks = [null, null, null, null];
var clientMax = 4;
var name = opt.options.name || 'localhost';
var port = opt.options.port || 12346;
var primary = {host: opt.options.Host, port: opt.options.Port || 12346};
var secondary;
var msgCount = 0;
var cli;
var wss;

var client_command = {
	connect: function (sock, msg) {
		if (msg.data && msg.data.type === 'secondary')
			secondary = {host: msg.data.host, port: msg.data.port};
		reply(sock, msg, register(null, msg, sock));
		sock.write(ugridMsg.encode({cmd: 'secondary', data: secondary || {}}));
		console.log('Connect ' + sock.client.data.type + ' ' +
					sock.client.index + ': ' + sock.client.uuid);
	},
	devices: function (sock, msg) {
		reply(sock, msg, devices(msg.data));
	},
	id: function (sock, msg) {
		reply(sock, msg, msg.data in clients ? clients[msg.data].index : null);
	},
	subscribe: function (sock, msg) {
		var publishers = msg.data, i;
		for (i in publishers)
			clients[publishers[i].uuid].subscribers.push(sock.client);
	},
	unsubscribe: function (sock, msg) {
		var publishers = msg.data, i, subscribers, sub;
		for (i in publishers) {
			subscribers = clients[publishers[i].uuid].subscribers;
			if ((sub = subscribers.indexOf(sock.client)) >= 0)
				subscribers.splice(sub, 1);
		}
	}
};

// Connect as backup to primary if provided
if (primary.host) {
	cli = new UgridClient({host: primary.host, port: primary.port,
						   data: {type: 'secondary', host: name, port: port}});
	cli.connect_cb(function () {
		console.log('connected as backup to ' + primary.host + ':' + primary.port);
		cli.send_cb(0, {cmd: 'secondary', id: 1, data: {host: name, port: port}});
		cli.on('end', function () {
			console.log('primary server connection closed');
		});
		cli.on('secondary', function () {});	// ignore broadcast from self
	});
}

// Start a websocket server if a listening port is specified on command line
if (opt.options.wsport) {
	wss = new webSocketServer({port: opt.options.wsport});
	wss.on('connection', function (ws) {
		console.log('websocket connect');
		var sock = handleConnect(websocket(ws));
		ws.on('close', function () {handleClose(sock);});
	});
}

// Start a TCP server
net.createServer(handleConnect).listen(port);

function handleConnect(sock) {
	var decoder = ugridMsg.Decoder();
	sock.setNoDelay(true);
	sock.pipe(decoder);

	decoder.on('Message', function (to, len, data) {
		var i, subscribers;
		try {
			msgCount++;
			if (to > 3) {			// Unicast
				if (!tsocks[to]) throw 'Invalid destination id: ' + to;
				tsocks[to].write(data);
			} else if (to == 2) {	// Broadcast
				for (i in tsocks)
					if (tsocks[i]) tsocks[i].write(data);
			} else if (to == 1) {	// Multicast
				subscribers = sock.client.subscribers;
				for (i in subscribers)
					if (tsocks[subscribers[i].index]) subscribers[i].sock.write(data);
			} else {				// Server request
				var o = JSON.parse(data.slice(8));
				if (!(o.cmd in client_command)) throw 'Invalid command: ' + o.cmd;
				client_command[o.cmd](sock, o);
			}
		} catch (error) {
			console.error('handleConnect error: ' + sock.client.data.type + ' ' +
						  sock.client.index + ': ' + error);
			console.error('Closing ' + sock.client.index + ': ' + sock.client.uuid);
			sock.end();
		}
	});
	sock.on('end', function () {handleClose(sock);});
	return sock;
}

function reply(sock, msg, data, error) {
	msg.cmd = 'reply';
	msg.from = 0;
	msg.data = data;
	msg.error = error;
	sock.write(ugridMsg.encode(msg));
}

function handleClose(sock) {
	var client = sock.client;
	if (!client) return;
	console.log('Disconnect ' + client.data.type + ' ' + client.index + ': ' + client.uuid);
	client.sock = tsocks[client.index] = null;
}

function register(from, msg, sock)
{
	var uuid = msg.uuid || uuidGen.v1(), index = clientMax++;
	sock.client = clients[uuid] = {
		index: index,
		uuid: uuid,
		owner: from ? from : uuid,
		data: msg.data || {},
		sock: sock,
		subscribers: []
	};
	tsocks[index] = sock;
	return {uuid: uuid, token: 0, id: index};
}

function devices(query) {
	var result = [];
	for (var i in clients) {
		if (!tsocks[clients[i].index])
			continue;
		var match = true;
		for (var j in query)
			if (!clients[i].data || clients[i].data[j] != query[j]) {
				match = false;
				break;
			}
		if (match)
			result.push({uuid: i, id: clients[i].index});
	}
	return result;
}

if (opt.options.statistics) {
	setInterval(function () {
		console.log('msg: ' + (msgCount / 5) + ' msg/s');
		msgCount = 0;
	}, 10000);
}
