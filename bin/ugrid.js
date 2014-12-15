#!/usr/local/bin/node

'use strict';

var net = require('net');
var uuidGen = require('node-uuid');
var ugridMsg = require('../lib/ugrid-msg.js');

var clients = {};
var tsocks = [null];
var clientMax = 1;
var port = process.argv[2] || 12346;
var c = 0;  // counter
var client_command = {
	connect: function (sock, msg) {
		msg.cmd = 'reply';
		msg.data = register(null, msg.data, sock);
		sock.write(ugridMsg.encode(msg));
		console.log('Connect ' + sock.client.data.type + ' ' +
					sock.client.index + ': ' + sock.client.uuid);
	},
	devices: function (sock, msg) {
		msg.cmd = 'reply';
		msg.data = devices(msg.data);
		sock.write(ugridMsg.encode(msg));
	}
};

net.createServer(function (sock) {
	var decoder = ugridMsg.Decoder();
	sock.pipe(decoder);

	decoder.on('Message', function (to, len, data) {
		try {
			c++;
			if (to === 0) {
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
			console.error(error);
			console.error('Closing ' + sock.client.index + ': ' + sock.client.uuid);
			sock.end();
		}
	});
	sock.on('end', function() {
		console.log('Disconnect ' + sock.client.data.type + ' ' +
		            sock.client.index + ': ' + sock.client.uuid);
		tsocks[sock.client.index] = null;
	});
}).listen(port);

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

setInterval(function() {
	console.log('c: ' + (c / 5) + ' msg/s');
	c = 0;
}, 10000);
