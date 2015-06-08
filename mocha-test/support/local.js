'use strict';

var ml = require('../../lib/ugrid-ml.js');

function Local(done) {
	if (!(this instanceof Local))
		return new Local(done);
	var data;

	//done(null, this);

	// Sources
	this.parallelize = function (v) {
		data = JSON.parse(JSON.stringify(v));
		return this;
	};

	// Actions
	this.collect = function (opt, done) {
		if (arguments.length < 2) done = opt;
		done(null, data);
	};

	this.count = function (done) {
		done(null, data.length);
	};

	this.countByValue = function (done) {
		var tmp = {}, str, i, out = [];
		for (i = 0; i < data.length; i++) {
			str = JSON.stringify(data[i]);
			if (tmp[str] === undefined) tmp[str] = [data[i], 0];
			tmp[str][1]++;
		}
		for (i in tmp) out.push(tmp[i]);
		done(null, out);
	};

	this.lookup = function(key, done) {
		done(null, data.filter(function (e) {return e[0] == key;}));
	};

	this.reduce = function(callback, initial, done) {
		done(null, data.reduce(callback, initial));
	};

	// Transformations
	this.distinct = function () {
		var out = [], ref = [];
		for (var i = 0; i < data.length; i++) {
			if (ref.indexOf(JSON.stringify(data[i])) != -1) continue;
			ref.push(JSON.stringify(data[i]));
			out.push(data[i]);
		}
		data = out;
		return this;
	};

	this.filter = function (filter) {
		data = data.filter(filter);
		return this;
	};

	this.flatMap = function (mapper) {
		data = data.map(mapper).reduce(function (a, b) {return a.concat(b);}, []);
		return this;
	};

	this.flatMapValues = function (mapper) {
		var i, out = [], t0;
		for (i = 0; i < data.length; i++) {
			t0 = mapper(data[i][1]);
			out = out.concat(t0.map(function (e) {return [data[i][0], e];}));
		}
		data = out;
		return this;
	};

	this.groupByKey = function () {
		var i, idx, keys = [], res = [];
		for (i = 0; i < data.length; i++)
			if (keys.indexOf(data[i][0]) == -1)
				keys.push(data[i][0]);
		for (i = 0; i < keys.length; i++) 
			res.push([keys[i], []]);
		for (i = 0; i < data.length; i++) {
			idx = keys.indexOf(data[i][0]);
			res[idx][1].push(data[i][1]);
		}
		data = res;
		return this;
	};

	this.keys = function () {
		data = data.map(function (e) {return e[0];});
		return this;
	};

	this.map = function (mapper) {
		data = data.map(mapper);
		return this;
	};

	this.mapValues = function (valueMapper) {
		data = data.map(function (e) {return [e[0], valueMapper(e[1])];});
		return this;
	};

	this.persist = function () {
		return this;
	};

	this.reduceByKey = function reduceByKey(reducer, init) {
		var i, idx, keys = [], res = [];
		for (i = 0; i < data.length; i++)
			if (keys.indexOf(data[i][0]) == -1)
				keys.push(data[i][0]);
		for (i = 0; i < keys.length; i++)
			res.push([keys[i], init]);
		for (i = 0; i < data.length; i++) {
			idx = keys.indexOf(data[i][0]);
			res[idx][1] = reducer(res[idx][1], data[i][1]);
		}
		data = res;
		return this;
	};

	this.sample = function (withReplacement, frac) {
		var P = 4, seed = 1;
		if (P > data.length) P = data.length;

		function split(a, n) {
			var len = a.length, out = [], i = 0;
			while (i < len) {
				var size = Math.ceil((len - i) / n--);
				out.push(a.slice(i, i += size))
			}
			return out;
		}	
		var map = split(data, P);

		var workerMap = [];
		for (var i = 0; i < P; i++) {
			workerMap[i] = {};
			workerMap[i][i] = map[i];
		}

		var out = [];
		for (var w = 0; w < P; w++) {
			var p = 0;
			var tmp = [];
			var rng = new ml.Random(seed);
			for (var i in workerMap[w]) {
				var L = workerMap[w][i].length;
				var L = Math.ceil(L * frac);
				tmp[p] = {data: []};
				var idxVect = [];
				while (tmp[p].data.length != L) {
					var idx = Math.round(Math.abs(rng.next()) * (L - 1));
					if ((idxVect.indexOf(idx) != -1) &&  !withReplacement) 
						continue;	// if already picked but no replacement mode
					idxVect.push[idx];
					tmp[p].data.push(workerMap[w][i][idx]);
				}
				out = out.concat(tmp[p].data)			
				p++;
			}
		}
		data = out;
		return this;
	};

	this.values = function () {
		data = data.map(function (e) {return e[1];});
		return this;
	};
}

var local = {};
module.exports = local;
local.context = Local;
