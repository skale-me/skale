'use strict';

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
LocalArray.prototype.parallelize = function (v) {
	this.data = JSON.parse(JSON.stringify(v));
	return this;
};

// Actions
LocalArray.prototype.collect = function (opt, done) {
	if (arguments.length < 2) done = opt;
	done(null, this.data);
};

LocalArray.prototype.count = function (done) {
	done(null, this.data.length);
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

LocalArray.prototype.values = function () {
	this.data = this.data.map(function (e) {return e[1];});
	return this;
};
