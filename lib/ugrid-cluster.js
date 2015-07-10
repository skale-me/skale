'use strict';

var fs = require('fs');
var net = require('net');
var trace = require('line-trace');
var SshClient = require('ssh2').Client;
var thenify = require('thenify').withCallback;

var defaultPort = 12346;
var defaultWsPort = 22346;
var workerPerHost = 4;
var poolFile = process.env.UGRID_POOL_FILE || 'machines.json';
var pool = JSON.parse(fs.readFileSync(poolFile, {encoding: 'utf8'}));
var clusters = {};
var clusterFile = process.env.UGRID_CLUSTER_FILE || __dirname + '/users/clusters.json';
var all = {};

// After loading machine files, set defaults value (number of instances, socket ports)
['server', 'controller', 'worker'].forEach(function (t) {
	for (var i = 0; i < pool[t].length; i++) {
		pool[t][i].num = 0;
		if (t == 'server') {
			pool[t][i].nextPort = defaultPort;
			pool[t][i].nextWsPort = defaultWsPort;
		}
	}
});

// Load saved cluster file if exists, and update state of machines
fs.readFile(clusterFile, {encoding: 'utf8'}, function (err, data) {
	var c, u, w, i, host, nw;
	if (err) return console.log('could not open ' + clusterFile);
	clusters = JSON.parse(data);
	for (u in clusters) {
		all[u] = {};
		for (c in clusters[u]) {
			all[u][c] = new Cluster(clusters[u][c]);
			all[u][c].test(function () {});

			host = clusters[u][c].server.host;
			for (i = 0; i < pool.server.length; i++) {
				if (pool.server[i].host == host) {
					pool.server[i].num++;
					if (clusters[u][c].port >= pool.server[i].nextPort)
						pool.server[i].nextPort = clusters[u][c].port + 1;
					if (clusters[u][c].wsport >= pool.server[i].nextWsPort)
						pool.server[i].nextWsPort = clusters[u][c].wsport + 1;
					break;
				}
			}
			host = clusters[u][c].controller.host;
			for (i = 0; i < pool.server.length; i++) {
				if (pool.controller[i].host == host) {
					pool.controller[i].num++;
					break;
				}
			}
			for (w = 0; w < clusters[u][c].worker.length; w++) {
				host = clusters[u][c].worker[w].host;
				nw = clusters[u][c].worker[w].nw;
				for (i = 0; i < pool.worker.length; i++) {
					if (pool.worker[i].host == host) {
						pool.worker[i].num += nw;
					}
					break;
				}
			}
		}
	}
});

var defaults = {
	user: process.env.USER,
	path: __dirname.replace(/\/[^\/]*$/, '/ugrid'),
	key: process.env.HOME + '/.ssh/id_rsa',
	nodebin: process.execPath
};

function machineSort(a, b) {return a.num > b.num;}

function saveCluster() {
	fs.writeFile(clusterFile, JSON.stringify(clusters), function (err) {
		if (err) throw err;
		console.log('cluster saved!');
	});
}

function Cluster(name, nbWorker, user) {
	if (!(this instanceof Cluster))
		return new Cluster(name, nbWorker, user);
	if (typeof arguments[0] == 'object') {
		// Create an instance from restored metadata
		this.cluster = arguments[0];
		return;
	}
	nbWorker = Number(nbWorker);
	var n = 0, remain = nbWorker;
	var cluster = this.cluster = {
		name: name,
		ram: nbWorker,
		host: pool.server[0].host,
		running: false,
		user: user,
		port: pool.server[0].nextPort++,
		wsport: pool.server[0].nextWsPort++,
		server: JSON.parse(JSON.stringify(pool.server[0])),
		controller: JSON.parse(JSON.stringify(pool.controller[0])),
		worker: []
	};

	if (!(user in clusters)) clusters[user] = {};
	clusters[user][name] = cluster;

	if (!(user in all)) all[user] = {};
	all[user][name] = this;
	
	// Allocate workers from machine pool, starting from the less busy (lowest num)
	// This involves sorting host arrays according to number of instances (num)
	do {
		n = remain < workerPerHost ? remain : workerPerHost;
		cluster.worker.push(JSON.parse(JSON.stringify(pool.worker[0])));
		pool.worker[0].num += n;
		cluster.worker[cluster.worker.length - 1].nw = n;
		pool.worker.sort(machineSort);
		remain -= n;
	} while (remain > 0);

	pool.server[0].num++;
	pool.controller[0].num++;
	pool.server.sort(machineSort);
	pool.controller.sort(machineSort);
	//console.log(pool);

	// Propagate default settings if needed
	['server', 'controller'].forEach(function (t) {
		['user', 'path', 'key', 'nodebin'].forEach(function (p) {
			cluster[t][p] = cluster[t][p] || pool[p] || defaults[p];
		});
	});
	for (var i = 0; i < cluster.worker.length; i++) {
		['user', 'path', 'key', 'nodebin'].forEach(function (p) {
			cluster.worker[i][p] = cluster.worker[i][p] || pool[p] || defaults[p];
		});
	}
	saveCluster();
}

function sshRun(host, cmd, callback) {
	var key = fs.readFileSync(host.key);
	var ssh = new SshClient();
	ssh.connect({
		host: host.host,
		port: 22,
		user: host.user,
		privateKey: key
	});
	ssh.on('ready', function () {
		trace(cmd);
		ssh.exec(cmd, function (err, stream) {
			stream.on('close', function (code, signal) {
				console.log('ssh cmd exit code: '+ code);
				ssh.end();
				callback();
			});
		});
	});
}

Cluster.prototype.start = thenify(function (callback) {
	var cluster = this.cluster;
	var name = cluster.user + ':' + cluster.name;
	var serverLog =  tmpDir('server') + '/ugrid-server-' + name + '.log';
	var serverPid =  tmpDir('server') + '/ugrid-server-' + name + '.pid';
	var serverCmd = cmdPrefix('server') + 'ugrid.js -p ' + cluster.port +
		' -w ' + cluster.wsport + ' > ' + serverLog + ' 2>&1 & echo $! >' + serverPid;

	var controllerLog = tmpDir('controller') + '/ugrid-controller-' + name + '.log';
	var controllerCmd = cmdPrefix('controller') + 'controller.js -H ' + cluster.server.host +
		' -P ' + cluster.port + ' > ' + controllerLog + ' 2>&1 &';

	sshRun(cluster.server, serverCmd, function (err, res) {
		console.log("server started");
		sshRun(cluster.controller, controllerCmd, function (err, res) {
			console.log("controller started");
			var remain = cluster.worker.length;
			for (var i = 0; i < cluster.worker.length; i++) {
				var worker = cluster.worker[i];
				var tmpdir = '/tmp/' + worker.user;
				var workerLog = tmpdir + '/ugrid-worker-' + name + '.log';
				var workerCmd = 'mkdir -p ' + tmpdir + '; ' + worker.nodebin + ' ' +
					worker.path + '/bin/' + 'worker.js -n ' + worker.nw + ' -m ' + worker.host + 
					' -H ' + cluster.server.host + ' -P ' + cluster.port + ' > ' + workerLog + ' 2>&1 &';
				sshRun(cluster.worker[i], workerCmd, function (err, res) {
					console.log("worker started");
					if (--remain == 0) callback();
				});
			}
		});
	});
	
	function tmpDir(type) {
		return '/tmp/' + cluster[type].user;
	}
	function cmdPrefix(type) {
		return 'mkdir -p ' + tmpDir(type) + '; ' + cluster[type].nodebin + ' ' +  cluster[type].path + '/bin/';
	}
});

Cluster.prototype.stop = thenify(function (callback) {
	var cluster = this.cluster;
	var name = cluster.user + ':' + cluster.name;
	var stopCmd = 'kill `cat ' + '/tmp/' + cluster.server.user + '/ugrid-server-' + name + '.pid`'
	sshRun(cluster.server, stopCmd, function (err, res) {
		console.log('cluster stopped');
		callback();
	});
});

Cluster.prototype.test = thenify(function (callback) {
	var cluster = this.cluster, sock;
	sock = net.connect({host: cluster.host, port: cluster.port}, function () {
		cluster.running = true;
		sock.end();
		callback(null, true);
	});
	sock.on('error', function () {cluster.running = false; callback(null, false);});
});

module.exports = Cluster;
module.exports.pool = pool;
module.exports.all = all;
