#!/usr/local/bin/node --harmony

var GridClient = require('../lib/ugrid-client.js');

var grid = new GridClient({
	host: 'localhost',
	port: 12346,
	data: {color: 'blue'}
});

grid.connect_cb(function(err, res) {
	if (err) {
		console.log(err);
		exit(0);
	}
	var my_uuid = res.uuid;
	console.log('Logged as #' + my_uuid);

	// register / unregister things
	grid.send_cb('register', {type: 'drone'}, function(err, res) {
		if (err)
			console.log(err)
		else
			console.log('Registered new thing as #' + res.uuid);
	});

	grid.send_cb('unregister', {uuid: '12345678'}, function(err, res) {
		if (err) console.log(err);
	});

	// subrscribe / unsubscribe to a thing identified with uuid
	grid.send_cb('subscribe', {uuid: "123456789"}, function(err, res) {
		if (err) console.log(err);
	});

	grid.send_cb('unsubscribe', {uuid: "123456789"}, function(err, res) {
		if (err) console.log(err);
	});

	// Publish messages (broadcast exemple)
	grid.send_cb('publish', {uuid: "*", payload: {hello: 'world'}}, function(err, res) {
		if (err) console.log(err);
	});

	// Receive messages
	grid.on('message', function(data) {
		console.log('Received new message')
		console.log(data);
	})

	// Query devices, get uuid array
	grid.send_cb('devices', {type: "drone"}, function(err, res) {
		if (err) console.log(err);
		console.log('drone type query');
		console.log(res);
	})

	// Get device data
	grid.send_cb('get', {uuid: my_uuid}, function(err, res) {
		if (err) console.log(err);
		console.log('my own data')
		console.log(res);
	})

	// Set device data
	grid.send_cb('set', {uuid: my_uuid, data: {type: 'arduino'}}, function(err, res) {
		if (err) console.log(err);
	})

	// Some examples
	// pub/sub loopback
	grid.send_cb('subscribe', {uuid: my_uuid}, function(err, res) {
		if (err) console.log(err);
		grid.send_cb('publish', {uuid: my_uuid, payload: {hello: 'world', from: 'myself'}}, function(err, res) {
			if (err) console.log(err);
		});
	});
	// Set my type to arduino and query my data
	grid.send_cb('set', {uuid: my_uuid, data: {type: 'arduino', weight: 'not that heavy'}}, function(err, res) {
		if (err) console.log(err);
		grid.send_cb('get', {uuid: my_uuid}, function(err, res) {
			if (err) console.log(err);
			console.log('my new data')
			console.log(res);
		})
	})	
});

// 	// use grid array
// 	var garray = new GridArray();
// 	garray.parallelize([1, 2, 3 , 4 , 5, 6]);
// 	garray.map(function(d) {return d * d}, function(res) {
// 		console.log(res);
// 	})
