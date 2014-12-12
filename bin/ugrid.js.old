#!/usr/local/bin/node --harmony

/*
	TODO:
		- Multicast publish modes
		- gestion des droits
		- REST API: publish subscribe
	BUG:
		- lancer new_ugrid.js et thing.js puis arreter new_ugrid.js et le relancer
*/

var net = require('net');
var readline = require('readline');
var uuid_gen = require('node-uuid');
var token_gen = require('rand-token');
var ugrid_server_rest = require('../lib/ugrid-server-rest.js');
var socket_port = process.argv[2] || 12346;
var rest_port = 4730;
var things = {};
var nb_dev = 0, msg_in = 0, msg_out = 0;

// ********************************************************************* //
// UGRID API
// ********************************************************************* //
var api = {
	authenticate: authenticate,
	register: register,
	unregister: unregister,
	query: query,
	get: get,
	set: set,
	things: things
};

function authenticate(uuid, token) {
	if (!(uuid in things) || (things[uuid].token != token))
		throw 'authentication failed';
}

function connect(from_uuid, data, connection) {
	if (from_uuid == null)
		return register(from_uuid, data, connection);
	things[from_uuid].online = true;
	things[from_uuid].connection = connection;
	things[from_uuid].data = data;
	return {uuid: from_uuid, token: things[from_uuid].token};
}

function register(from_uuid, data, connection) {
	var uuid = uuid_gen.v1();
	var token = token_gen.generate(32);
	things[uuid] = {
		uuid: uuid,
		token: token,
		owner: from_uuid ? from_uuid : uuid, 
		online: false, 
		data: data, 
		subscribers: []
	};
	if (connection) {		
		things[uuid].connection = connection;
		if (things[uuid].connection.protocol == 'TCP') {
			things[uuid].connection.sock.uuid = uuid;
			things[uuid].online = true;
		}
	}
	return {uuid: uuid, token: things[uuid].token};
}

function unregister(from_uuid, uuid) {
	if (!(uuid in things))
		throw 'unregister error: device not found';
	if (things[uuid].owner != from_uuid)
		throw 'unregister error: not owner';
	delete things[uuid];
	return null;
}

function subscribe(from_uuid, uuid) {
	if (!(uuid in things))
		throw 'subscribe error: device not found';
	things[uuid].subscribers.push(from_uuid);
	return null;
}

function unsubscribe(from_uuid, uuid) {
	if (!(uuid in things))
		throw 'unsubscribe error: device not found';
	var idx;
	if ((idx = things[data.uuid].subscribers.indexOf(from_uuid)) != -1)
		things[uuid].subscribers.splice(idx, 1);
	return null;
}

function send_thing(from_uuid, to_uuid, cmd_id, data, cmd, line) {
	if (!(to_uuid in things)) throw 'send_thing: device not found: ' + to_uuid;
	var thing = things[to_uuid];
	if (!(thing.online)) return;
	var msg = {from: from_uuid, cmd_id: cmd_id, data: data.payload};
	if (cmd) msg.cmd = cmd;
	if (line) {
		console.log(line);
		console.log(JSON.stringify(msg));
	}
	thing.connection.sock.write(JSON.stringify(msg) + '\n');
	++msg_out;
}

function publish(from_uuid, data, cmd_id) {
	if (data.uuid == '*') {     // Broadcast
		for (var i = 0; i < things[from_uuid].subscribers.length; i++) {
			send_thing(from_uuid, things[from_uuid].subscribers[i], cmd_id, data, 'message');
		}
	} else {                     // Single cast
		send_thing(from_uuid, data.uuid, cmd_id, data, 'message');
	}
}

// Pb: can not query on meta-data (i.e online status)
function query(from_uuid, query) {
	var uuids = [];
	for (var i in things) {
		if (!things[i].online) continue;
		var match = true;
		for (var j in query)
			if (things[i].data[j] != query[j]) {
				match = false;
				break;
			}
		if (match)
			uuids.push(i);
	}
	return {devices: uuids};
}

function get(from_uuid, uuid) {
	if (!(uuid in things))
		throw 'get error: device not found';
	return things[uuid].data;
}

function set(from_uuid, uuid, data) {
	if (!(uuid in things))
		throw 'set error: device not found';
	for (i in data) {
		if ((data[i] == null) || (data[i] == 'null')) // if null value remove key from device data
			delete things[uuid].data[i];
		else	// else update/add field
			things[uuid].data[i] = data[i];
	}
	return things[uuid].data;
}

// ********************************************************************* //
// TCP-SOCKET API
// ********************************************************************* //
var client_command = {
	connect: function(sock, msg, line) {
		nb_dev++;
		send(sock, msg, connect(msg.from, msg.data, {protocol: "TCP", sock: sock}));
	},
	register: function(sock, msg, line) {
		send(sock, msg, register(msg.from, msg.data));
	},
	unregister: function(sock, msg, line) {
		send(sock, msg, unregister(msg.from, msg.data.uuid));
	},
	subscribe: function(sock, msg, line) {
		send(sock, msg, subscribe(msg.from, msg.data.uuid));
	},
	unsubscribe: function(sock, msg, line) {
		send(sock, msg, unsubscribe(msg.from, msg.data.uuid));
	},
	publish: function(sock, msg, line) {
		send(sock, msg, publish(msg.from, msg.data, msg.cmd_id));
	},
	request: function(sock, msg, line) {
		send(sock, msg, send_thing(msg.from, msg.data.uuid, msg.cmd_id, msg.data, 'request', line));
	},
	answer: function(sock, msg, line) {
		send(sock, msg, send_thing(msg.from, msg.data.uuid, msg.cmd_id, msg.data, line));
	},
	devices: function(sock, msg, line) {
		send(sock, msg, query(msg.from, msg.data));
	},
	get: function(sock, msg, line) {
		send(sock, msg, get(msg.from, msg.data.uuid));
	},
	set: function(sock, msg, line) {
		send(sock, msg, set(msg.from, msg.data.uuid, msg.data.data));
	}
}

function send(sock, msg, result) {
	// Todo: handle destination error
	if (msg.cmd === 'request') return;
	var json = {cmd_id: msg.cmd_id, data: {result: result}};
	sock.write(JSON.stringify(json) + '\n');
	msg_out++;
}

var grid = net.createServer(function(sock) {
	var rl = readline.createInterface(sock, sock);

	rl.on('line', function(d) {
		try {
			var o = JSON.parse(d);
			++msg_in;
			//console.log(o);
			//console.log("");
			// Authentification
			//var isConnect = (o.cmd == 'connect');
			//var isAuthenticated = (o.from != undefined) || (o.token  != undefined);
			//if (!(isConnect))
			//	authenticate(o.from, o.token);
			//if (isAuthenticated)
			//	authenticate(o.from, o.token);
			client_command[o.cmd](sock, o, d);
		} catch (error) {
			console.log(error);
			var json = {cmd_id: o.cmd_id, data: {err: error, result: null}};
			if (error == 'authentication failed') {
				sock.end(JSON.stringify(json) + '\n');
				sock.destroy();
			} else
				sock.write(JSON.stringify(json) + '\n');
			// sock.end(JSON.stringify(json) + '\n');
			// sock.destroy()
		}
	});

	sock.on('end', function() {
		if (sock.uuid in things)
			things[sock.uuid].online = false;
		console.log('disconnected: ' + sock.uuid);
		nb_dev--;
	});
});

grid.listen(socket_port);
setInterval(function() {
	console.log('devices: ' + nb_dev + '\tmsg in: ' + msg_in + ', out: ' + msg_out);
	msg_in = msg_out = 0;
}, 1000);

ugrid_server_rest(api, process.env.UGRID_REST_PORT || rest_port);
