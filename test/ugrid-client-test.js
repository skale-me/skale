var child_process = require('child_process')
var fs = require('fs')
var stream = require('stream')
var UgridClient = require('../lib/ugrid-client.js')

var uc, server

describe('ugrid', function () {
	before(function (done) {
		var output
		server = child_process.spawn('./bin/ugrid.js')
		server.stdout.once('data', function (d) {
			if (output) return
			output = true
			uc = new UgridClient({type: 'test'})
			done()
		})
	})

	after(function () {
		process.kill(server.pid)
	})

	describe('UgridClient', function() {
		it('a server should be running', function (done) {
			console.assert(server.pid > 0)
			done()
		})
		it('should be an UgridClient object', function (done) {
			console.assert(uc instanceof UgridClient)
			done()
		})
		it('should connect to server', function (done) {
			uc.on('connect', function (msg) {
				done()
			})
		})
	})

	describe('Client streams', function () {
		var gstream
		it('createStreamTo should return a stream', function (done) {
			var g1 = uc.createStreamTo({from: 4, streamId: 4})
			console.assert(g1 instanceof stream.Stream)
			done()
		})
		it('createStreamFrom should return a stream', function (done) {
			var g1 = uc.createStreamFrom(uc.uuid, {cmd: 'dummmy'})
			console.assert(g1 instanceof stream.Stream)
			done()
		})
		it('should send and answer to request', function (done) {
			uc.on('fileRequest', function (msg) {
				var gstream = uc.createStreamTo(msg)
				var instream = fs.createReadStream('/etc/hosts')
				instream.pipe(gstream)
			})
			var gs = uc.createStreamFrom(uc.uuid, {cmd: 'fileRequest'})
			gs.pipe(fs.createWriteStream('/dev/null'))
			gs.on('end', done)
		})
	})
})
