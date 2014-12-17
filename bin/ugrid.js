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
var tsocks = [null];
var clientMax = 1;
var name = opt.options.name || 'localhost';
var port = opt.options.port || 12346;
var primary = {host: opt.options.Host, port: opt.options.Port || 12346};
var secondary;
var msgCount = 0;
var cli;
var wss;

var client_command = {
	connect: function (sock, msg) {
		msg.cmd = 'reply';
		msg.data = register(null, msg.data, sock);
		sock.write(ugridMsg.encode(msg));
		sock.write(ugridMsg.encode({cmd: 'broadcast', data: {secondary: secondary || {}}}));
		console.log('Connect ' + sock.client.data.type + ' ' +
					sock.client.index + ': ' + sock.client.uuid);
	},
	devices: function (sock, msg) {
		msg.cmd = 'reply';
		msg.data = devices(msg.data);
		sock.write(ugridMsg.encode(msg));
	},
	broadcast: broadcast
};

// Connect as backup to primary if provided
if (primary.host) {
	cli = new UgridClient({host: primary.host, port: primary.port, data: {type: 'backup'}});
	cli.connect_cb(function () {
		console.log('connected as backup to ' + primary.host + ':' + primary.port);
		cli.send_cb({cmd: 'broadcast', data: {secondary: {host: name, port: port}}});
		cli.on('end', function () {
			console.log('primary server connection closed');
		});
		cli.on('broadcast', function(o) {});	// ignore self broadcast
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
	sock.pipe(decoder);

	decoder.on('Message', function (to, len, data) {
		try {
			msgCount++;
			if (!to) {
				var o = JSON.parse(data.slice(8));
				if (!(o.cmd in client_command))
                    throw 'Invalid command: ' + o.cmd;
				client_command[o.cmd](sock, o);
			}
			else {
				if (!tsocks[to])
					throw 'Invalid destination id: ' + to;
				tsocks[to].write(data);
			}
		} catch (error) {
			console.error('handleConnect error: ' + error);
			console.error('Closing ' + sock.client.index + ': ' + sock.client.uuid);
			sock.end();
		}
	});
	sock.on('end', function () {handleClose(sock);});
	return sock;
}

function handleClose(sock) {
	console.log('Disconnect ' + sock.client.data.type + ' ' +
				sock.client.index + ': ' + sock.client.uuid);
	tsocks[sock.client.index] = null;
}

function register(from, data, sock)
{
	var uuid = uuidGen.v1(), index = clientMax++;
	sock.client = clients[uuid] = {
		index: index,
		uuid: uuid,
		owner: from ? from : uuid,
		data: data || {},
		sock: sock
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

function broadcast(sock, msg) {
	if ('secondary' in msg.data)
		secondary = msg.data.secondary;
	var d = ugridMsg.encode(msg);
	for (var i in tsocks) {
		if (tsocks[i]) {
			tsocks[i].write(d);
		}
	}
}

if (opt.options.statistics) {
	setInterval(function() {
		console.log('msg: ' + (msgCount / 5) + ' msg/s');
		msgCount = 0;
	}, 10000);
}
