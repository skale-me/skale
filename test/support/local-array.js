'use strict';

var fs = require('fs');
var stream = require('stream');
var util = require('util');
var trace = require('line-trace');
var Lines = require('../../lib/lines.js');
var ml = require('../../lib/ugrid-ml.js');

module.exports = LocalArray;
module.exports.TextStream = TextStream;

function LocalArray() {
	if (!(this instanceof LocalArray))
		return new LocalArray();
}

// Sources
LocalArray.prototype.lineStream = function (inputStream, opt) {
	this.stream = inputStream.pipe(new Lines()).pipe(new BlockStream(opt.N));
	return this;
};

LocalArray.prototype.parallelize = function (v) {
	this.data = JSON.parse(JSON.stringify(v));
	return this;
};

LocalArray.prototype.textFile = function (path) {
	var self = this, raw = fs.readFileSync(path, {encoding: 'utf8'});
	this.data = [];
	raw.split('\n').map(function (s) {if (!s) return; self.data.push(s);});
	return this;
};

// Actions
LocalArray.prototype.collect = function (opt, done) {
	opt = opt || {};
	if (arguments.length < 2) done = opt;
	if (opt.stream) return this.stream;
	if (this.stream) {
		var res = [];
		this.stream.on('data', function (data) {res = res.concat(data);});
		this.stream.on('end', function () {done(null, res);});
	} else
		done(null, this.data);
};

LocalArray.prototype.count = function (opt, done) {
	if (arguments.length < 2) done = opt;
	if (opt.stream) {
		this.stream = this.stream.pipe(new CountStream());
		return this.stream;
	}
	if (this.stream) {
		var res = 0;
		this.stream = this.stream.pipe(new CountStream());
		this.stream.on('data', function (data) {res += data;});
		this.stream.on('end', function () {done(null, res);});
	} else
		done(null, this.data.length);
};

LocalArray.prototype.countByValue = function (opt, done) {
	if (arguments.length < 2) done = opt;
	if (opt.stream) {
		this.stream = this.stream.pipe(new CountByValueStream());
		return this.stream;
	}
	if (this.stream) {
		var res = [];
		this.stream = this.stream.pipe(new CountByValueStream());
		this.stream.on('data', function (data) {res = res.concat(data);});
		this.stream.on('end', function () {done(null, res);});
		return this.stream;
	}
	var tmp = {}, str, i, out = [];
	for (i = 0; i < this.data.length; i++) {
		str = JSON.stringify(this.data[i]);
		if (tmp[str] === undefined) tmp[str] = [this.data[i], 0];
		tmp[str][1]++;
	}
	for (i in tmp) out.push(tmp[i]);
	done(null, out);
};

LocalArray.prototype.lookup = function(key, opt, done) {
	if (arguments.length < 3) done = opt;
	if (opt.stream) {
		this.stream = this.stream.pipe(new lookupStream(key));
		return this.stream;
	}
	if (this.stream) {
		var res = [];
		this.stream = this.stream.pipe(new LookupStream(key));
		this.stream.on('data', function (data) {res = res.concat(data);});
		this.stream.on('end', function () {done(null, res);});
		return this.stream;
	} 
	done(null, this.data.filter(function (e) {return e[0] == key;}));
};

LocalArray.prototype.reduce = function(callback, initial, opt, done) {
	if (arguments.length < 4) done = opt;
	if (opt.stream) {
		this.stream = this.stream.pipe(new ReduceStream(callback, initial));
		return this.stream;
	}
	if (this.stream) {
		var res = [];
		this.stream = this.stream.pipe(new ReduceStream(callback, initial));
		this.stream.on('data', function (data) {res = res.concat(data);});
		this.stream.on('end', function () {done(null, res);});
		return this.stream;
	} 
	done(null, this.data.reduce(callback, initial));
};

// Transformations
LocalArray.prototype.coGroup = function (other) {
	if (this.stream) {
		this.stream = this.stream.pipe(new DualTransformStream(other, coGroup));
	} else if (other.stream) {
		this.stream = new DualTransformStream(other, coGroup);
		this.stream.end(this.data);
	} else {
		this.data = coGroup(this.data, other.data);
	}
	return this;
}

LocalArray.prototype.crossProduct = function (other) {
	if (this.stream) {
		this.stream = this.stream.pipe(new DualTransformStream(other, crossProduct));
	} else if (other.stream) {
		this.stream = new DualTransformStream(other, crossProduct);
		this.stream.end(this.data);
	} else {
		this.data = crossProduct(this.data, other.data);
	}
	return this;
};

LocalArray.prototype.distinct = function () {
	if (this.stream)
		this.stream = this.stream.pipe(new DistinctStream());
	else
		this.data = distinct(this.data);
	return this;
};

LocalArray.prototype.filter = function (filter) {
	if (this.stream)
		this.stream = this.stream.pipe(new FilterStream(filter));
	else
		this.data = this.data.filter(filter);
	return this;
};

LocalArray.prototype.flatMap = function (mapper) {
	if (this.stream)
		this.stream = this.stream.pipe(new FlatMapStream(mapper));
	else
		this.data = this.data.map(mapper).reduce(function (a, b) {return a.concat(b);}, []);
	return this;
};

LocalArray.prototype.flatMapValues = function (mapper) {
	if (this.stream)
		this.stream = this.stream.pipe(new FlatMapValuesStream(mapper));
	else
		this.data = flatMapValues(this.data, mapper);
	return this;
};

LocalArray.prototype.groupByKey = function () {
	if (this.stream)
		this.stream = this.stream.pipe(new GroupByKeyStream());
	else
		this.data = groupByKey(this.data);
	return this;
};

LocalArray.prototype.intersection = function (other) {
	if (this.stream) {
		this.stream = this.stream.pipe(new DualTransformStream(other, intersection));
	} else if (other.stream) {
		this.stream = new DualTransformStream(other, intersection);
		this.stream.end(this.data);
	} else {
		this.data = intersection(this.data, other.data);
	}
	return this;
};

LocalArray.prototype.join = function (other) {
	if (this.stream) {
		this.stream = this.stream.pipe(new DualTransformStream(other, join));
	} else if (other.stream) {
		this.stream = new DualTransformStream(other, join);
		this.stream.end(this.data);
	} else {
		this.data = join(this.data, other.data);
	}
	return this;
};

LocalArray.prototype.keys = function () {
	if (this.stream)
		this.stream = this.stream.pipe(new KeysStream());
	else
		this.data = this.data.map(function (e) {return e[0];});
	return this;
};

LocalArray.prototype.leftOuterJoin = function (other) {
	if (this.stream) {
		this.stream = this.stream.pipe(new DualTransformStream(other, leftOuterJoin));
	} else if (other.stream) {
		this.stream = new DualTransformStream(other, leftOuterJoin);
		this.stream.end(this.data);
	} else {
		this.data = leftOuterJoin(this.data, other.data);
	}
	return this;
};

LocalArray.prototype.map = function (mapper) {
	if (this.stream)
		this.stream = this.stream.pipe(new MapStream(mapper));
	else
		this.data = this.data.map(mapper);
	return this;
};

LocalArray.prototype.mapValues = function (valueMapper) {
	if (this.stream)
		this.stream = this.stream.pipe(new MapValuesStream(valueMapper));
	else
		this.data = this.data.map(function (e) {return [e[0], valueMapper(e[1])];});
	return this;
};

LocalArray.prototype.persist = function () {
	return this;
};

LocalArray.prototype.reduceByKey = function (reducer, init) {
	if (this.stream)
		this.stream = this.stream.pipe(new ReduceByKeyStream(reducer, init));
	else
		this.data = reduceByKey(this.data, reducer, init);
	return this;
};

LocalArray.prototype.rightOuterJoin = function (other) {
	if (this.stream) {
		this.stream = this.stream.pipe(new DualTransformStream(other, rightOuterJoin));
	} else if (other.stream) {
		this.stream = new DualTransformStream(other, rightOuterJoin);
		this.stream.end(this.data);
	} else {
		this.data = rightOuterJoin(this.data, other.data);
	}
	return this;
};

LocalArray.prototype.sample = function (withReplacement, frac) {
	if (this.stream) {
		this.stream = this.stream.pipe(new SampleStream(withReplacement, frac));
		return this;
	}
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
	if (this.stream) {
		this.stream = this.stream.pipe(new DualTransformStream(other, subtract));
	} else if (other.stream) {
		this.stream = new DualTransformStream(other, subtract);
		this.stream.end(this.data);
	} else {
		this.data = subtract(this.data, other.data);
	}
	return this;
};

LocalArray.prototype.union = function (other) {
	if (this.stream) {
		this.stream = this.stream.pipe(new DualTransformStream(other, union));
	} else if (other.stream) {
		this.stream = new DualTransformStream(other, union);
		this.stream.end(this.data);
	} else {
		this.data = union(this.data, other.data);
	}
	return this;
}

LocalArray.prototype.values = function () {
	if (this.stream)
		this.stream = this.stream.pipe(new ValuesStream());
	else
		this.data = this.data.map(function (e) {return e[1];});
	return this;
};

// Streams
function BlockStream(len) {
	if (!(this instanceof BlockStream))
		return new BlockStream(len);
	stream.Transform.call(this, {objectMode: true});
	this.len = len;
	this.cnt = 0;
	this.buf = [];
}
util.inherits(BlockStream, stream.Transform);

BlockStream.prototype._transform = function (msg, encoding, done) {
	this.buf.push(msg);
	if (++this.cnt == this.len) {
		this.push(this.buf);
		this.buf = [];
		this.cnt = 0;
	}
	done();
};

// dual transform
function DualTransformStream(other, action) {
	if (!(this instanceof DualTransformStream))
		return new DualTransformStream(other);
	stream.Transform.call(this, {objectMode: true});
	this.other = other;
	this.action = action;
	var self = this;
	if (this.other.stream) {
		this.other.stream.pause();
		this.other.stream.on('end', function () {
			self.otherEnd = true;
		});
	}
}
util.inherits(DualTransformStream, stream.Transform);

DualTransformStream.prototype._transform = function (msg, encoding, done) {
	var otherStream = this.other.stream, action = this.action;
	if (otherStream) {
		var data = otherStream.read();
		if (data !== null)
			done(null, action(data, msg));
		else if (this.otherEnd)
			done(null, msg);
		else {
			otherStream.once('readable', function () {
				var data = otherStream.read();
				done(null, action(msg, data));
			});
		}
	} else
		done(null, action(msg, this.other.data));
		//this.other.data = undefined;
};

DualTransformStream.prototype._flush = function (done) {
	var self = this;
	if (this.other.stream) {
		this.other.stream.resume();
		this.other.stream.on('data', function (d) {self.push(self.action(d, null));});
		this.other.stream.on('end', done);
	} else done();
}

// Count
function CountStream() {
	if (!(this instanceof CountStream))
		return new CountStream();
	stream.Transform.call(this, {objectMode: true});
	this.count = 0;
}
util.inherits(CountStream, stream.Transform);

CountStream.prototype._transform = function (msg, encoding, done) {
	done(null, msg.length);
};

// CountByValue
function CountByValueStream() {
	if (!(this instanceof CountByValueStream))
		return new CountByValueStream();
	stream.Transform.call(this, {objectMode: true});
}
util.inherits(CountByValueStream, stream.Transform);

CountByValueStream.prototype._transform = function (msg, encoding, done) {
	var tmp = {}, str, i, out = [];
	for (i = 0; i < msg.length; i++) {
		str = JSON.stringify(msg[i]);
		if (tmp[str] === undefined) tmp[str] = [msg[i], 0];
		tmp[str][1]++;
	}
	for (i in tmp) out.push(tmp[i]);
	done(null, out);
};

// Distinct
function DistinctStream() {
	if (!(this instanceof DistinctStream))
		return new DistinctStream();
	stream.Transform.call(this, {objectMode: true});
}
util.inherits(DistinctStream, stream.Transform);

DistinctStream.prototype._transform = function (msg, encoding, done) {
	done(null, distinct(msg));
};

// Filter
function FilterStream(filter) {
	if (!(this instanceof FilterStream))
		return new FilterStream(filter);
	stream.Transform.call(this, {objectMode: true});
	this.filter = filter;
}
util.inherits(FilterStream, stream.Transform);

FilterStream.prototype._transform = function (msg, encoding, done) {
	done(null, msg.filter(this.filter));
};

// FlatMap
function FlatMapStream(mapper) {
	if (!(this instanceof FlatMapStream))
		return new FlatMapStream(mapper);
	stream.Transform.call(this, {objectMode: true});
	this.mapper = mapper;
}
util.inherits(FlatMapStream, stream.Transform);

FlatMapStream.prototype._transform = function (msg, encoding, done) {
	done(null, msg.map(this.mapper).reduce(function (a, b) {return a.concat(b);}, []));
};

// FlatMapValues
function FlatMapValuesStream(mapper) {
	if (!(this instanceof FlatMapValuesStream))
		return new FlatMapValuesStream(mapper);
	stream.Transform.call(this, {objectMode: true});
	this.mapper = mapper;
}
util.inherits(FlatMapValuesStream, stream.Transform);

FlatMapValuesStream.prototype._transform = function (msg, encoding, done) {
	done (null, flatMapValues(msg, this.mapper));
};

// GroupByKey
function GroupByKeyStream() {
	if (!(this instanceof GroupByKeyStream))
		return new GroupByKeyStream();
	stream.Transform.call(this, {objectMode: true});
}
util.inherits(GroupByKeyStream, stream.Transform);

GroupByKeyStream.prototype._transform = function (msg, encoding, done) {
	done(null, groupByKey(msg));
};

// Keys
function KeysStream() {
	if (!(this instanceof KeysStream))
		return new KeysStream();
	stream.Transform.call(this, {objectMode: true});
}
util.inherits(KeysStream, stream.Transform);

KeysStream.prototype._transform = function (msg, encoding, done) {
	done(null, msg.map(function (e) {return e[0];}));
};

// Lookup
function LookupStream(key) {
	if (!(this instanceof LookupStream))
		return new LookupStream(key);
	stream.Transform.call(this, {objectMode: true});
	this.key = key;
}
util.inherits(LookupStream, stream.Transform);

LookupStream.prototype._transform = function (msg, encoding, done) {
	var key = this.key;
	done(null, msg.filter(function (e) {return e[0] == key;}));
};

// Map
function MapStream(mapper) {
	if (!(this instanceof MapStream))
		return new MapStream(mapper);
	stream.Transform.call(this, {objectMode: true});
	this.mapper = mapper;
}
util.inherits(MapStream, stream.Transform);

MapStream.prototype._transform = function (msg, encoding, done) {
	done(null, msg.map(this.mapper));
};

// MapValues
function MapValuesStream(mapper) {
	if (!(this instanceof MapValuesStream))
		return new MapValuesStream(mapper);
	stream.Transform.call(this, {objectMode: true});
	this.mapper = mapper;
}
util.inherits(MapValuesStream, stream.Transform);

MapValuesStream.prototype._transform = function (msg, encoding, done) {
	var mapper = this.mapper;
	done(null, msg.map(function (e) {return [e[0], mapper(e[1])];}));
};

// Reduce
function ReduceStream(callback, initial) {
	if (!(this instanceof ReduceStream))
		return new ReduceStream(callback, initial);
	stream.Transform.call(this, {objectMode: true});
	this.callback = callback;
	this.initial = initial;
}
util.inherits(ReduceStream, stream.Transform);

ReduceStream.prototype._transform = function (msg, encoding, done) {
	var data = JSON.parse(JSON.stringify(msg));
	done(null, data.reduce(this.callback, this.initial));
};

// ReduceByKey
function ReduceByKeyStream(reducer, init) {
	if (!(this instanceof ReduceByKeyStream))
		return new ReduceByKeyStream(callback, initial);
	stream.Transform.call(this, {objectMode: true});
	this.reducer = reducer;
	this.init = init;
}
util.inherits(ReduceByKeyStream, stream.Transform);

ReduceByKeyStream.prototype._transform = function (msg, encoding, done) {
	done(null, reduceByKey(msg, this.reducer, this.init));
};

// Sample
function SampleStream(withReplacement, frac) {
	if (!(this instanceof SampleStream))
		return new SampleStream(withReplacement, frac);
	stream.Transform.call(this, {objectMode: true});
	this.withReplacement = withReplacement;
	this.frac = frac;
}
util.inherits(SampleStream, stream.Transform);

// XXXXX FIXME: implement sampling in stream
SampleStream.prototype._transform = function (msg, encoding, done) {
	done(null, msg);
};

// Text
function TextStream() {
	if (!(this instanceof TextStream))
		return new TextStream();
	stream.Transform.call(this, {objectMode: true});
}
util.inherits(TextStream, stream.Transform);

TextStream.prototype._transform = function (msg, encoding, done) {
	done(null, msg.toString());
};

// Union
function UnionStream(other) {
	if (!(this instanceof UnionStream))
		return new UnionStream(other);
	stream.Transform.call(this, {objectMode: true});
	this.other = other;
	var self = this;
	if (this.other.stream) {
		this.other.stream.on('end', function () {
			self.otherEnd = true;
		});
	}
}
util.inherits(UnionStream, stream.Transform);

UnionStream.prototype._transform = function (msg, encoding, done) {
	if (this.other.stream) {
		var data = this.other.stream.read();
		if (data !== null)
			done(null, [].concat(msg, data));
		else if (this.otherEnd)
			done(null, msg);
		else
			this.other.stream.once('readable', function () {
				done(null, [].concat(msg, this.read()));
			});
	} else
		done(null, [].concat(msg, this.other.data));
		//this.other.data = undefined;
};

UnionStream.prototype._flush = function (done) {
	var self = this;
	if (this.other.stream) {
		this.other.stream.on('data', function (d) {self.push(d);});
		this.other.stream.on('end', done);
	} else done();
}

// Values
function ValuesStream() {
	if (!(this instanceof ValuesStream))
		return new ValuesStream();
	stream.Transform.call(this, {objectMode: true});
}
util.inherits(ValuesStream, stream.Transform);

ValuesStream.prototype._transform = function (msg, encoding, done) {
	done(null, msg.map(function (e) {return e[1];}));
};

// Helper functions
function coGroup(v1, v2) {
	var v = [], already_v1 = [], already_v2 = [];

	for (var i = 0; i < v1.length; i++)
		for (var j = 0; j < v2.length; j++)
			if (v1[i][0] == v2[j][0]) {
				var idx = -1;
				for (var k = 0; k < v.length; k++) {
					if (v[k][0] == v1[i][0]) {
						idx = k;
						break;
					}
				}
				if (idx == -1) {
					idx = v.length;
					v[v.length] = [v1[i][0], [[], []]];
				}
				if (!already_v1[i]) {
					v[idx][1][0].push(v1[i][1]);
					already_v1[i] = true;
				}
				if (!already_v2[j]) {
					v[idx][1][1].push(v2[j][1]);
					already_v2[j] = true;
				}
			}
	return v;
}

function crossProduct(v1, v2) {
	var v = [], i, j;
	for (i = 0; i < v1.length; i++)
		for (j = 0; j < v2.length; j++)
			v.push([v1[i], v2[j]])
	return v;
}

function distinct(v) {
	var out = [], ref = {}, s;
	for (var i = 0; i < v.length; i++) {
		s = JSON.stringify(v[i]);
		//if (ref.indexOf(JSON.stringify(this.data[i])) != -1) continue;
		//ref.push(JSON.stringify(this.data[i]));
		if (s in ref) continue;
		ref[s] = true;
		out.push(v[i]);
	}
	return out;
}

function flatMapValues(v, mapper) {
	var i, out = [], t0;
	for (i = 0; i < v.length; i++) {
		t0 = mapper(v[i][1]);
		out = out.concat(t0.map(function (e) {return [v[i][0], e];}));
	}
	return out;
}

function groupByKey(v) {
	var i, idx, keys = [], out = [];
	for (i = 0; i < v.length; i++)
		if (keys.indexOf(v[i][0]) == -1)
			keys.push(v[i][0]);
	for (i = 0; i < keys.length; i++)
		out.push([keys[i], []]);
	for (i = 0; i < v.length; i++) {
		idx = keys.indexOf(v[i][0]);
		out[idx][1].push(v[i][1]);
	}
	return out;
}

function intersection(v1, v2) {
	var e, i, j, v = [];
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
	return v;
}

function leftOuterJoin(v1, v2) {
	var i, j, found, v = [];
	for (i = 0; i < v1.length; i++) {
		found = false;
		for (j = 0; j < v2.length; j++) {
			if (v1[i][0] == v2[j][0]) {
				found = true;
				v.push([v1[i][0], [v1[i][1], v2[j][1]]]);
			}
		}
		if (!found)
			v.push([v1[i][0], [v1[i][1], null]]);
	}
	return v;
}

function join(v1, v2) {
	var i, j, found, v = [];
	for (i = 0; i < v1.length; i++)
		for (j = 0; j < v2.length; j++)
			if (v1[i][0] == v2[j][0])
				v.push([v1[i][0], [v1[i][1], v2[j][1]]])
	return v;
}

function reduceByKey(v, reducer, init) {
	var i, idx, keys = [], res = [];
	for (i = 0; i < v.length; i++)
		if (keys.indexOf(v[i][0]) == -1)
			keys.push(v[i][0]);
	for (i = 0; i < keys.length; i++)
		res.push([keys[i], init]);
	for (i = 0; i < v.length; i++) {
		idx = keys.indexOf(v[i][0]);
		res[idx][1] = reducer(res[idx][1], v[i][1]);
	}
	return res;
}

function rightOuterJoin(v1, v2) {
	var i, j, found, v = [];
	for (i = 0; i < v2.length; i++) {
		found = false;
		for (j = 0; j < v1.length; j++) {
			if (v2[i][0] == v1[j][0]) {
				found = true;
				v.push([v2[i][0], [v1[j][1], v2[i][1]]]);
			}
		}
		if (!found)
			v.push([v2[i][0], [null, v2[i][1]]]);
	}
	return v;
}

function subtract(v1, v2) {
	var v = [], e, i, j, found, s1 = v1.map(JSON.stringify), s2 = v2.map(JSON.stringify);
	for (i = 0; i < s1.length; i++) {
		found = false;
		for (j = 0; j < s2.length; j++)
			if (s2[j] == s1[i]) {
				found = true;
				break;
			}
		if (!found)
			v.push(v1[i]);
	}
	return v;
}

function union(v1, v2) {
	return v1.concat(v2);
}
