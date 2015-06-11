'use strict';

var fs = require('fs');
//var Lines = require('../../lib/lines.js');
var ml = require('../../lib/ugrid-ml.js');

module.exports = LocalArray;

function join(first, other, type) {
	var i, j, found, v3 = [];
	if (type === undefined) {
		for (i = 0; i < first.length; i++)
			for (j = 0; j < other.length; j++)
				if (first[i][0] == other[j][0])
					v3.push([first[i][0], [first[i][1], other[j][1]]])
	} else if (type == 'left') {
		for (i = 0; i < first.length; i++) {
			found = false;
			for (j = 0; j < other.length; j++) {
				if (first[i][0] == other[j][0]) {
					found = true;
					v3.push([first[i][0], [first[i][1], other[j][1]]]);
				}
			}
			if (!found)
				v3.push([first[i][0], [first[i][1], null]]);
		}
	} else if (type == 'right') {
		for (i = 0; i < other.length; i++) {
			found = false;
			for (j = 0; j < first.length; j++) {
				if (other[i][0] == first[j][0]) {
					found = true;
					v3.push([other[i][0], [first[j][1], other[i][1]]]);
				}
			}
			if (!found)
				v3.push([other[i][0], [null, other[i][1]]]);
		}
	}
	return v3;
}

function LocalArray() {
	if (!(this instanceof LocalArray))
		return new LocalArray();
}

// Sources
LocalArray.prototype.lineStream = function(inputStream) {
	//this.stream = new Lines();
	//this.data = [];
	//var self = this;
	//inputStream.pipe(this.stream);
	//this.stream.on('data', function (d) { self.data.push(d) });

	var self = this, raw = fs.readFileSync(inputStream.path, {encoding: 'utf8'});
	this.data = [];
	raw.split('\n').map(function (s) {self.data.push(s);});
	return this;
};

LocalArray.prototype.parallelize = function (v) {
	this.data = JSON.parse(JSON.stringify(v));
	return this;
};

LocalArray.prototype.textFile = function (path) {
	var self = this, raw = fs.readFileSync(path, {encoding: 'utf8'});
	this.data = [];
	raw.split('\n').map(function (s) {self.data.push(s);});
	return this;
};

// Actions
LocalArray.prototype.coGroup = function (other) {
	var v1 = this.data;
	var v2 = other.data;
	var v3 = [];
	var already_v1 = [];
	var already_v2 = [];

	for (var i = 0; i < v1.length; i++)
		for (var j = 0; j < v2.length; j++)
			if (v1[i][0] == v2[j][0]) {
				var idx = -1;
				for (var k = 0; k < v3.length; k++) {
					if (v3[k][0] == v1[i][0]) {
						idx = k;
						break;
					}
				}
				if (idx == -1) {
					idx = v3.length;
					v3[v3.length] = [v1[i][0], [[], []]];
				}
				if (!already_v1[i]) {
					v3[idx][1][0].push(v1[i][1]);
					already_v1[i] = true;
				}
				if (!already_v2[j]) {
					v3[idx][1][1].push(v2[j][1]);
					already_v2[j] = true;
				}
			}
	this.data = v3;
	return this;
}

LocalArray.prototype.collect = function (opt, done) {
	if (arguments.length < 2) done = opt;
	done(null, this.data);
};

LocalArray.prototype.count = function (done) {
	done(null, this.data.length);
};

LocalArray.prototype.crossProduct = function (other) {
	var v1 = this.data, v2 = other.data, v3 = [], i, j;
	for (i = 0; i < v1.length; i++)
		for (j = 0; j < v2.length; j++)
			v3.push([v1[i], v2[j]])
	this.data = v3;
	return this;
};

LocalArray.prototype.countByValue = function (done) {
	var tmp = {}, str, i, out = [];
	for (i = 0; i < this.data.length; i++) {
		str = JSON.stringify(this.data[i]);
		if (tmp[str] === undefined) tmp[str] = [this.data[i], 0];
		tmp[str][1]++;
	}
	for (i in tmp) out.push(tmp[i]);
	done(null, out);
};

LocalArray.prototype.lookup = function(key, done) {
	done(null, this.data.filter(function (e) {return e[0] == key;}));
};

LocalArray.prototype.reduce = function(callback, initial, done) {
	done(null, this.data.reduce(callback, initial));
};

// Transformations
LocalArray.prototype.distinct = function () {
	var out = [], ref = [];
	for (var i = 0; i < this.data.length; i++) {
		if (ref.indexOf(JSON.stringify(this.data[i])) != -1) continue;
		ref.push(JSON.stringify(this.data[i]));
		out.push(this.data[i]);
	}
	this.data = out;
	return this;
};

LocalArray.prototype.filter = function (filter) {
	this.data = this.data.filter(filter);
	return this;
};

LocalArray.prototype.flatMap = function (mapper) {
	this.data = this.data.map(mapper).reduce(function (a, b) {return a.concat(b);}, []);
	return this;
};

LocalArray.prototype.flatMapValues = function (mapper) {
	var i, out = [], t0, self = this;
	for (i = 0; i < this.data.length; i++) {
		t0 = mapper(this.data[i][1]);
		out = out.concat(t0.map(function (e) {return [self.data[i][0], e];}));
	}
	this.data = out;
	return this;
};

LocalArray.prototype.groupByKey = function () {
	var i, idx, keys = [], res = [];
	for (i = 0; i < this.data.length; i++)
		if (keys.indexOf(this.data[i][0]) == -1)
			keys.push(this.data[i][0]);
	for (i = 0; i < keys.length; i++)
		res.push([keys[i], []]);
	for (i = 0; i < this.data.length; i++) {
		idx = keys.indexOf(this.data[i][0]);
		res[idx][1].push(this.data[i][1]);
	}
	this.data = res;
	return this;
};

LocalArray.prototype.intersection = function (other) {
	var e, i, j, v = [], v1 = this.data, v2 = other.data;
	for (i = 0; i < v1.length; i++) {
		e = JSON.stringify(v1[i]);
		if (v.indexOf(e) != -1) continue;
		for (j = 0; j < v2.length; j++) {
			if (JSON.stringify(v2[j]) == e) {
				v.push(v1[i]);
				break;
			}
		}
	}
	this.data = v;
	return this;
};

LocalArray.prototype.join = function (other) {
	this.data = join(this.data, other.data);
	return this;
};

LocalArray.prototype.keys = function () {
	this.data = this.data.map(function (e) {return e[0];});
	return this;
};

LocalArray.prototype.leftOuterJoin = function (other) {
	this.data = join(this.data, other.data, 'left');
	return this;
};

LocalArray.prototype.map = function (mapper) {
	this.data = this.data.map(mapper);
	return this;
};

LocalArray.prototype.mapValues = function (valueMapper) {
	this.data = this.data.map(function (e) {return [e[0], valueMapper(e[1])];});
	return this;
};

LocalArray.prototype.persist = function () {
	return this;
};

LocalArray.prototype.reduceByKey = function reduceByKey(reducer, init) {
	var i, idx, keys = [], res = [];
	for (i = 0; i < this.data.length; i++)
		if (keys.indexOf(this.data[i][0]) == -1)
			keys.push(this.data[i][0]);
	for (i = 0; i < keys.length; i++)
		res.push([keys[i], init]);
	for (i = 0; i < this.data.length; i++) {
		idx = keys.indexOf(this.data[i][0]);
		res[idx][1] = reducer(res[idx][1], this.data[i][1]);
	}
	this.data = res;
	return this;
};

LocalArray.prototype.rightOuterJoin = function (other) {
	this.data = join(this.data, other.data, 'right');
	return this;
};

LocalArray.prototype.sample = function (withReplacement, frac) {
	var P = 4, seed = 1;
	if (P > this.data.length) P = this.data.length;

	function split(a, n) {
		var len = a.length, out = [], i = 0;
		while (i < len) {
			var size = Math.ceil((len - i) / n--);
			out.push(a.slice(i, i += size))
		}
		return out;
	}
	var map = split(this.data, P);

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
	this.data = out;
	return this;
};

LocalArray.prototype.subtract = function (other) {
	var v1 = this.data, v2 = other.data, v = [], e, i, j, found;
	for (i = 0; i < v1.length; i++) {
		e = JSON.stringify(v1[i]);
		found = false;
		for (j = 0; j < v2.length; j++)
			if (JSON.stringify(v2[j]) == e) {
				found = true;
				break;
			}
		if (!found)
			v.push(v1[i]);
	}
	this.data = v;
	return this;
};

LocalArray.prototype.union = function (other) {
	this.data = this.data.concat(other.data);
	return this;
}

LocalArray.prototype.values = function () {
	this.data = this.data.map(function (e) {return e[1];});
	return this;
};
