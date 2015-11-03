<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [UgridClient class](#ugridclient-class)
- [wsConnect_cb([callback])](#wsconnect_cb-callback)
- [connect_cb([callback])](#connect_cb-callback)
- [disconnect()](#disconnect)
- [send_cb(uuid, object, [callback])](#send_cb-uuid-object-callback)
- [devices_cb(object, [number], [callback])](#devices_cb-object-number-callback)
- [on(event, callback(msg))](#on-event-callback-msg)
- [publish_cb(data, [callback])](#publish_cb-data-callback)
- [request_cb(host, data, [callback])](#request_cb-host-data-callback)
- [reply_cb(msg, error, data, [callback])](#reply_cb-msg-error-data-callback)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## UgridClient class
Example:

	var UgridClient = require('ugrid-client');
	var grid = new UgridClient({
		host: 'localhost',
		port: 12346,
		data: {type: 'worker'}
	});

## wsConnect_cb([callback])
Example:

	grid.wsConnect_cb(function (err, res) {
		console.log("Connected as " + ' ' + res.uuid);
	};

## connect_cb([callback])
Example:

	grid.connect_cb(function (err, res) {
		console.log("Connected as " + ' ' + res.uuid);
	};

## disconnect()
Example:

	grid.disconnect();

## send_cb(uuid, object, [callback])
Example:

	grid.send_cb(0, {cmd: 'hello', data: "world"}, function (err, res) {
		console.log("message sent");
	});

## devices_cb(object, [number], [callback])
Example:

	grid.devices_cb({type: 'worker'}, function (err, res) {
		console.log(res.length + " workers are connected");
	});

## on(event, callback(msg))
Example:

	grid.on("hello", callback(msg) {
		grid.reply_cb(msg, null, "Welcome");
	});

## publish_cb(data, [callback])
Example:

   grid.publish_cb(count++);

## request_cb(host, data, [callback])
Example:

	grid.devices_cb({type: 'worker'}, 2, function (err, res) {
		grid.request_cb(worker[1], "hello", function (err2, res2) {
			console.log("response from worker[1]: " + res2);
		});
	});

## reply_cb(msg, error, data, [callback])
