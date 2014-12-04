var express = require('express');
var bodyParser = require('body-parser');

// ********************************************************************* //
// REST API
// ********************************************************************* //
var app = express();
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({extended: true}));
var api;

// authentication filter
app.all('*', function(req, res, next) {
	// DEBUG: bypass auth for base uri
	if (req.path == '/')
		return next();

	// authentification non requise pour methode POST vers /devices
	var isRegister = (req.path == '/devices') && (req.method == 'POST');
	var isAuthenticated = (req.headers.auth_token != undefined) || (req.headers.auth_uuid  != undefined);
	if (!(isRegister))
		api.authenticate(req.headers.auth_uuid, req.headers.auth_token);
	if (isAuthenticated)
		api.authenticate(req.headers.auth_uuid, req.headers.auth_token);
	next();
})

// POST /devices : register thing
app.post('/devices', function(req, res) {
	res.send(api.register(req.headers.auth_uuid, req.body));
});

// GET /devices/:uuid, get device data
app.get('/devices/:uuid', function(req, res) {
	res.send(api.get(req.headers.auth_uuid, req.params.uuid));
});

// PUT /devices/:uuid, set device data
app.put('/devices/:uuid', function(req, res) {
	res.send(api.set(req.headers.auth_uuid, req.params.uuid, req.body));
});

// GET /devices : query devices matching criteria
app.get('/devices', function(req, res) {
	res.send(api.query(req.headers.auth_uuid, req.query));
});

// DELETE /devices/:uuid, unregister device
app.delete('/devices/:uuid', function(req, res){
	res.send(api.unregister(req.headers.auth_uuid, req.params.uuid))
});

// DEBUG GET / : list all devices with details
app.get('/', function(req, res) {
	var json = {};
	for (i in api.things) {
		json[i] = {};
		for (j in api.things[i])
			json[i][j] = (j == 'connection') ? api.things[i][j].protocol : api.things[i][j];
	}
	res.send(json);
});

// POST /messages : publish message
// curl -X POST -d '{"uuid": "*", "payload": {"yellow":"off"}}' http://localhost:4730/messages -H "Content-Type: application/json"
// app.post('/messages', function(req, res) {
// 	// passer le from uuid ici
// 	publishMessage(req.body, null);
// 	res.send({});
// });

//app.listen(process.env.PORT || rest_port);

module.exports = function(ugrid_api, port) {
	api = ugrid_api;
	app.listen(port);	
}
