var ml = require('./ugrid-ml.js');

function Count() {
	this.result = 0;

	this.pipeline = function(array) {
		this.result += array.length;
	}
}

function Collect() {
	this.result = {};

	this.pipeline = function(array, p) {
		if (this.result[p] == undefined) this.result[p] = [];
		var dest = this.result[p];
		for (var i = 0; i < array.length; i++)
			dest.push(array[i]);
	}
}

function Reduce(action) {
	this.result = action.args[0];
	var reduce = action.src;

	this.pipeline = function(array) {
		for (var i = 0; i < array.length; i++)
			this.result = reduce(this.result, array[i]);
	}
}

function Lookup(action) {
	this.result = {};
	var key = action.args[0];

	this.pipeline = function (array, p) {
		if (this.result[p] == undefined) this.result[p] = [];
		var dest = this.result[p];
		for (var i = 0; i < array.length; i++)
			if (array[i][0] == key) dest.push(array[i]);
	}
}

function CountByValue() {
	this.result = {};

	this.pipeline = function(array) {
		for (var i = 0; i < array.length; i++) {
			var str = JSON.stringify(array[i]);
			if (this.result[str] == undefined)
				this.result[str] = [array[i], 0];
			this.result[str][1]++;
		}
	}
}

function ForEach(action) {
	var each = action.src;

	this.pipeline = function(array) {
		array.forEach(each);
	}
}

function Union(node) {
	this.pipeline = function (array, p) {return array;}
}

function Map(node) {
	var mapper = node.src;
	var args = node.args;

	this.pipeline = function(array) {
		for (var i = 0; i < array.length; i++)
			array[i] = mapper(array[i], args[0]);
		return array;
	}
}

function Filter(node) {
	var filter = node.src;

	this.pipeline = function(array) {
		var tmp = [];
		for (var i = 0; i < array.length; i++)
			if (filter(array[i])) tmp.push(array[i]);
		return tmp;
	}
}

function FlatMap(node) {
	var mapper = node.src;

	this.pipeline = function(array) {
		var tmp = [];
		for (var i = 0; i < array.length; i++)
			tmp = tmp.concat(mapper(array[i]));
		return tmp;
	}
}

function MapValues(node) {
	var mapper = node.src;

	this.pipeline = function(array) {
		for (var i = 0; i < array.length; i++) 
			array[i][1] = mapper(array[i][1]);
		return array;
	}
}

function Sample(node) {
	var withReplacement = node.args[0];
	var frac = node.args[1];
	var seed = node.args[2];
	this.SRAM = [];
	var tmp = {};

	this.pipeline = function(array, p) {
		if (tmp[p] == undefined) tmp[p] = [];
		for (var i = 0; i < array.length; i++)
			tmp[p].push(array[i]);
	}

	this.tx_shuffle = function(state, worker) {
		var p = 0;
		var rng = new ml.Random(seed);
		for (var i in tmp) {
			var L = Math.ceil(tmp[i].length * frac);
			var data = [];
			var idxVect = [];
			while (data.length != L) {
				var idx = Math.round(Math.abs(rng.next()) * (L - 1));
				if (!withReplacement && (idxVect.indexOf(idx) != -1))
					continue;	// if already picked but no replacement mode
				idxVect.push[idx];
				data.push(tmp[i][idx]);
			}
			this.SRAM[p++] = {data: data};
		}
		state.nShuffle = worker.length;	// Cancel shuffle
	}
}

function GroupByKey(node) {
	this.result = {};
	var SRAM = this.SRAM = [];

	this.pipeline = function(array) {
		var dest = this.result;
		for (var i = 0; i < array.length; i++) {
			var key = array[i][0];
			if (dest[key] == undefined) dest[key] = [[key, []]];
			dest[key][0][1].push(array[i][1]);
		}
	}

	this.tx_shuffle = function(state, worker, grid) {
		var map = worker.map(function() {return {};});
		for (var p in this.result)
			map[ml.cksum(p) % worker.length][p] = this.result[p];
		for (var i = 0; i < map.length; i++)
			if (grid.host.uuid == worker[i].uuid) this.rx_shuffle(map[i], state);
			else grid.request_cb(worker[i], {cmd: 'shuffle', args: map[i]}, function(err) {if (err) throw err;});
	}

	this.rx_shuffle = function(data, state) {
		for (var key in data) {
			for (var i = 0; i < SRAM.length; i++)
				if (SRAM[i].key == key) break;
			if (i == SRAM.length) SRAM.push({key: key, data: data[key]});
			else SRAM[i].data[0][1] = SRAM[i].data[0][1].concat(data[key][0][1]);
		}
		state.nShuffle++;
	}
}

function ReduceByKey(node) {
	this.result = {};
	var reducer = node.src;
	var initVal = node.args[0];
	var SRAM = this.SRAM = [];

	this.pipeline = function(array) {
		var dest = this.result;
		for (var i = 0; i < array.length; i++) {
			var key = array[i][0];
			if (dest[key] == undefined)
				dest[key] = [[key, JSON.parse(JSON.stringify(initVal))]];
			dest[key][0][1] = reducer(dest[key][0][1], array[i][1]);
		}
	}

	this.tx_shuffle = function(state, worker, grid) {
		var map = worker.map(function() {return {};});
		for (var p in this.result)
			map[ml.cksum(p) % worker.length][p] = this.result[p];
		for (var i = 0; i < map.length; i++)
			if (grid.host.uuid == worker[i].uuid) this.rx_shuffle(map[i], state);
			else grid.request_cb(worker[i], {cmd: 'shuffle', args: map[i]}, function(err) {if (err) throw err;});
	}

	this.rx_shuffle = function(data, state) {
		for (var key in data) {
			for (var i = 0; i < SRAM.length; i++)
				if (SRAM[i].key == key) break;
			if (i == SRAM.length) SRAM.push({key: key, data: data[key]});
			else SRAM[i].data[0][1] = reducer(SRAM[i].data[0][1], data[key][0][1]);
		}
		state.nShuffle++;
	}
}

function Join(node) {
	this.result = {};
	var SRAM = this.SRAM = {};

	this.pipeline = function(array, p, src_id) {
		var dest = this.result;
		for (var i = 0; i < array.length; i++) {
			var key = array[i][0];
			if (dest[key] == undefined) dest[key] = {key: key};
			if (dest[key][src_id] == undefined) dest[key][src_id] = [];
			dest[key][src_id].push(array[i][1]);
		}
	}

	this.tx_shuffle = function(state, worker, grid) {
		var map = worker.map(function() {return {};});
		for (var p in this.result)
			map[ml.cksum(p) % worker.length][p] = this.result[p];
		for (var i = 0; i < map.length; i++)
			if (grid.host.uuid == worker[i].uuid) this.rx_shuffle(map[i], state, worker);
			else grid.request_cb(worker[i], {cmd: 'shuffle', args: map[i]}, function(err) {if (err) throw err;});
	}
	this.rx_shuffle = function(data, state, worker) {
		for (var i in data)
			if (i in SRAM)
				for (var k in data[i]) {
					if (k == 'key') continue;
					if (SRAM[i][k] == undefined) SRAM[i][k] = data[i][k];
					else SRAM[i][k] = SRAM[i][k].concat(data[i][k]);
				}
			else SRAM[i] = data[i];
		// When all shuffle has been received
		if (++state.nShuffle < worker.length) return;
		var res = [];
		for (var i in SRAM) {
			var datasets = Object.keys(SRAM[i]);
			if (datasets.length != 3) continue;
			var t0 = [];
			for (var j = 0; j < SRAM[i][node.child[0]].length; j++)
				for (var k = 0; k < SRAM[i][node.child[1]].length; k++)
					t0.push([SRAM[i].key, [SRAM[i][node.child[0]][j], SRAM[i][node.child[1]][k]]])
			res.push({key: SRAM[i].key, data: t0})
		}
		this.SRAM = res;
	}
}

function CoGroup(node) {
	this.result = {};
	var SRAM = this.SRAM = {};

	this.pipeline = function(array, p, src_id) {
		var dest = this.result;
		for (var i = 0; i < array.length; i++) {
			var key = array[i][0];
			if (dest[key] == undefined) dest[key] = {key: key};
			if (dest[key][src_id] == undefined) dest[key][src_id] = [];
			dest[key][src_id].push(array[i][1]);
		}
	}

	this.tx_shuffle = function(state, worker, grid) {
		var map = worker.map(function() {return {};});
		for (var p in this.result)
			map[ml.cksum(p) % worker.length][p] = this.result[p];
		for (var i = 0; i < map.length; i++)
			if (grid.host.uuid == worker[i].uuid) this.rx_shuffle(map[i], state, worker);
			else grid.request_cb(worker[i], {cmd: 'shuffle', args: map[i]}, function(err) {if (err) throw err;});
	}

	this.rx_shuffle = function(data, state, worker) {
		for (var i in data)
			if (i in SRAM) {
				for (var k in data[i]) {
					if (k == 'key') continue;
					if (SRAM[i][k] == undefined) SRAM[i][k] = data[i][k];
					else SRAM[i][k] = SRAM[i][k].concat(data[i][k]);
				}
			} else SRAM[i] = data[i];
		// When all shuffle has been received
		if (++state.nShuffle < worker.length) return;
		var res = [];
		for (var i in SRAM) {
			var datasets = Object.keys(SRAM[i]);
			if (datasets.length != 3) continue;
			res.push({
				key: SRAM[i].key,
				data:[[SRAM[i].key, [SRAM[i][node.child[0]], SRAM[i][node.child[1]]]]]
			})
		}
		this.SRAM = res;
	}
}

function CrossProduct(node) {
	this.result = {};
	var SRAM = this.SRAM = {};

	this.pipeline = function(array, p, src_id) {
		var dest = this.result;
		if (dest[src_id] == undefined) dest[src_id] = [];
		if (dest[src_id][p] == undefined) dest[src_id][p] = [];
		for (var i = 0; i < array.length; i++)
			dest[src_id][p].push(array[i]);
	}

	this.tx_shuffle = function(state, worker, grid) {
		var map = [], k = 0;
		var keys = Object.keys(this.result);
		var t0 = this.result[keys[0]].length;
		var t1 = this.result[keys[1]].length;
		var map = new Array(worker.length);
		while (t0 && t1) {
			if (map[k] == undefined) {
				map[k] = {};
				map[k][keys[0]] = [];
				map[k][keys[1]] = [];
			}
			map[k][keys[0]].push(this.result[keys[0]][this.result[keys[0]].length - t0--]);
			map[k][keys[1]].push(this.result[keys[1]][this.result[keys[1]].length - t1--]);
			k = (k + 1) % worker.length;
		}
		for (var i = 0; i < map.length; i++)
			if (grid.host.uuid == worker[i].uuid) this.rx_shuffle(map[i] || {}, state, worker);
			else grid.request_cb(worker[i], {cmd: 'shuffle', args: map[i] || {}}, function(err) {if (err) throw err;});
	}

	this.rx_shuffle = function(data, state, worker) {
		for (var i in data)
			if (i in SRAM)
				for (var j = 0; j < data[i].length; j++) SRAM[i].push(data[i][j]);
			else SRAM[i] = data[i];
		// When all shuffle has been received
		if (++state.nShuffle < worker.length) return;
		if (Object.keys(SRAM).length == 0) return;
		var res = [];
		function crossProduct(a, b) {
			var t0 = [];
			for (var i = 0; i < a.length; i++)
				for (var j = 0; j < b.length; j++)
					t0.push([a[i], b[j]]);
			return t0;
		}
		var a = SRAM[node.child[0]];
		var b = SRAM[node.child[1]];
		for (var i = 0; i < a.length; i++)
			for (var j = 0; j < b.length; j++)
				res.push({data: crossProduct(a[i], b[j])})
		this.SRAM = res;
	}
}

function Distinct(node) {
	this.result = {};
	this.SRAM = [];

	this.pipeline = function(array, p, src_id, worker) {
		loop:
		for (var i = 0; i < array.length; i++) {
			var str = JSON.stringify(array[i]);
			var wid = ml.cksum(str) % worker.length;
			if (this.result[wid] == undefined) {
				this.result[wid] = [];
				this.result[wid][p] = {data: [str]};
			} else {
				for (var j = 0; j < this.result[wid].length; j++)
					if (this.result[wid][j].data.indexOf(str) != -1) continue loop;
				this.result[wid][p].data.push(str);
			}
		}
	}

	this.tx_shuffle = function(state, worker, grid) {
		for (var i = 0; i < worker.length; i++) {
			if (worker[i].uuid == grid.host.uuid) this.rx_shuffle(this.result[i], state, worker);
			else grid.request_cb(worker[i], {cmd: 'shuffle', args: this.result[i] || {}}, function(err) {if (err) throw err;});
		}
	}

	this.rx_shuffle = function (data, state, worker) {
		if (data == undefined) {
			++state.nShuffle;
			return;
		}
		var t0 = [];
		for (var i = 0; i < data.length; i++) {
			var elements = [];
			loop:
			for (var j = 0; j < data[i].data.length; j++) {
				for (var k = 0; k < this.SRAM.length; k++)
					if (this.SRAM[k].data.indexOf(data[i].data[j]) != -1) continue loop;
				elements.push(data[i].data[j]);
			}
			if (elements.length) t0.push({data: elements});
		}
		this.SRAM = this.SRAM.concat(t0);
		// When all shuffle has been received
		if (++state.nShuffle < worker.length) return;
		for (var i = 0; i < this.SRAM.length; i++)
			for (var j = 0; j < this.SRAM[i].data.length; j++)
				this.SRAM[i].data[j] = JSON.parse(this.SRAM[i].data[j]);
	}
}

function Intersection(node, worker) {
	this.SRAM = [];

	var tmp = {};
	tmp[node.child[0]] = [];
	tmp[node.child[1]] = [];
	var map = worker.map(function() {return {};});
	for (var i = 0; i < worker.length; i++) {
		map[i][node.child[0]] = [];
		map[i][node.child[1]] = [];		
	}

	this.pipeline = function(array, p, src_id) {
		for (var i = 0; i < array.length; i++) {
			var str = JSON.stringify(array[i]);
			var wid = ml.cksum(str) % worker.length;
			map[wid][src_id].push(str);
		}
	}

	this.tx_shuffle = function(state, worker, grid) {
		for (var i = 0; i < map.length; i++)
			if (grid.host.uuid == worker[i].uuid) this.rx_shuffle(map[i], state, worker);
			else grid.request_cb(worker[i], {cmd: 'shuffle', args: map[i]}, function(err) {if (err) throw err;});
	}

	this.rx_shuffle = function(data, state, worker) {
		for (var i in data) tmp[i].push(data[i]);
		if (++state.nShuffle < worker.length) return;
		var result = [];
		for (var i = 0; i < tmp[node.child[0]].length; i++)
			loop:
			for (var j = 0; j < tmp[node.child[0]][i].length; j++) {
				var e = tmp[node.child[0]][i][j];
				if (result.indexOf(e) != -1) continue;
				// Rechercher e dans toutes les partitions de child[1]
				for (var k = 0; k < tmp[node.child[1]].length; k++)
					if (tmp[node.child[1]][k].indexOf(e) != -1) {
						result.push(e);
						continue loop;
					}
			}
		this.SRAM = [{data: []}];
		for (var i = 0; i < result.length; i++)
			this.SRAM[0].data.push(JSON.parse(result[i])); 
	}
}

function Substract(node, worker) {
	this.SRAM = [];

	var tmp = {};
	tmp[node.child[0]] = [];
	tmp[node.child[1]] = [];
	var map = worker.map(function() {return {};});
	for (var i = 0; i < worker.length; i++) {
		map[i][node.child[0]] = [];
		map[i][node.child[1]] = [];		
	}

	this.pipeline = function(array, p, src_id) {
		for (var i = 0; i < array.length; i++) {
			var str = JSON.stringify(array[i]);
			var wid = ml.cksum(str) % worker.length;
			map[wid][src_id].push(str);
		}
	}

	this.tx_shuffle = function(state, worker, grid) {
		for (var i = 0; i < map.length; i++)
			if (grid.host.uuid == worker[i].uuid) this.rx_shuffle(map[i], state, worker);
			else grid.request_cb(worker[i], {cmd: 'shuffle', args: map[i]}, function(err) {if (err) throw err;});
	}

	this.rx_shuffle = function(data, state, worker) {
		for (var i in data) tmp[i] = tmp[i].concat(data[i]);
		if (++state.nShuffle < worker.length) return;
		var v1 = tmp[node.child[0]];
		var v2 = tmp[node.child[1]];
		var v_ref = [];
		for (var i = 0; i < v1.length; i++) {
			if (v_ref.indexOf(v1[i]) != -1) continue;
			if (v2.indexOf(v1[i]) != -1) continue;
			v_ref.push(v1[i]);
		}
		this.SRAM = [{data: v_ref.map(JSON.parse)}];
	}
}

function FlatMapValues(node) {
	var mapper = node.src;

	this.pipeline = function(array) {
		var tmp = [];
		for (var i = 0; i < array.length; i++) {
			var t0 = mapper(array[i][1]);
			tmp = tmp.concat(t0.map(function(e) {return [array[i][0], e]}));
		}
		return tmp;
	}
}

module.exports.count = Count;
module.exports.collect = Collect;
module.exports.reduce = Reduce;
module.exports.lookup = Lookup;
module.exports.countByValue = CountByValue;
module.exports.forEach = ForEach;
module.exports.union = Union;
module.exports.map = Map;
module.exports.filter = Filter;
module.exports.flatMap = FlatMap;
module.exports.mapValues = MapValues;
module.exports.sample = Sample;
module.exports.groupByKey = GroupByKey;
module.exports.reduceByKey = ReduceByKey;
module.exports.join = Join;
module.exports.coGroup = CoGroup;
module.exports.crossProduct = CrossProduct;
module.exports.distinct = Distinct;
module.exports.intersection = Intersection;
module.exports.substract = Substract;
module.exports.flatMapValues = FlatMapValues;
