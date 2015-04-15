'use strict';

var ml = require('./ugrid-ml.js');

module.exports.takeOrdered = function(grid, action) {
	var num = action.args[0];
	var sorter = action.args[1];
	this.result = [];

	this.pipeline = function(array) {
		for (var i = 0; i < array.length; i++) {
			this.result.push(array[i]);
			this.result = this.result.sort(sorter);
			this.result = this.result.slice(0, num);
		}
	}
}

module.exports.top = function(grid, action) {
	var num = action.args[0];
	var sorter = action.args[1];
	this.result = [];

	this.pipeline = function(array) {
		for (var i = 0; i < array.length; i++) {
			this.result.push(array[i]);
			this.result = this.result.sort(sorter);
			this.result = this.result.slice(0, num);
		}
	}
}

module.exports.take = function(grid, action) {
	this.result = [];
	var num = action.args[0];

	this.pipeline = function(array) {
		for (var i = 0; i < array.length; i++)
			if (this.result.length < num) this.result.push(array)
			else break;
	}
}

module.exports.count = function(grid) {
	this.result = 0;

	this.pipeline = function(array) {
		this.result += array.length;
	}
}

module.exports.collect = function(grid) {
	this.result = {};

	this.pipeline = function(array, p, head) {
		if (this.result[p] == undefined) this.result[p] = [];
		var dest = this.result[p];
		if (head)
			for (var i = array.length - 1; i >= 0; i--) dest.unshift(array[i]);			
		else
			for (var i = 0; i < array.length; i++) dest.push(array[i]);			
	}
}

module.exports.reduce = function(grid, action) {
	this.result = action.args[0];
	var reduce = action.src;

	this.pipeline = function(array) {
		for (var i = 0; i < array.length; i++)
			this.result = reduce(this.result, array[i]);
	}
}

module.exports.lookup = function(grid, action) {
	this.result = {};
	var key = action.args[0];

	this.pipeline = function (array, p) {
		if (this.result[p] == undefined) this.result[p] = [];
		var dest = this.result[p];
		for (var i = 0; i < array.length; i++)
			if (array[i][0] == key) dest.push(array[i]);
	}
}

module.exports.countByValue = function(grid) {
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

module.exports.forEach = function(grid, action) {
	var each = action.src;

	this.pipeline = function(array) {
		array.forEach(each);
	}
}

module.exports.union = function(grid, node) {
	this.pipeline = function (array, p) {return array;}
}

module.exports.map = function(grid, node) {
	var mapper = node.src;
	var args = node.args;

	this.pipeline = function(array) {
		for (var i = 0; i < array.length; i++)
			array[i] = mapper(array[i], args[0]);
		return array;
	}
}

module.exports.filter = function(grid, node) {
	var filter = node.src;

	this.pipeline = function(array) {
		var tmp = [];
		for (var i = 0; i < array.length; i++)
			if (filter(array[i])) tmp.push(array[i]);
		return tmp;
	}
}

module.exports.flatMap = function(grid, node) {
	var mapper = node.src;

	this.pipeline = function(array) {
		var tmp = [];
		for (var i = 0; i < array.length; i++)
			tmp = tmp.concat(mapper(array[i]));
		return tmp;
	}
}

module.exports.mapValues = function(grid, node) {
	var mapper = node.src;

	this.pipeline = function(array) {
		for (var i = 0; i < array.length; i++)
			array[i][1] = mapper(array[i][1]);
		return array;
	}
}

module.exports.sample = function(grid, node, worker) {
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

	this.tx_shuffle = function(state) {
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
				idxVect.push(idx);
				data.push(tmp[i][idx]);
			}
			this.SRAM[p++] = {data: data};
		}
		state.nShuffle = worker.length;	// Cancel shuffle
	}
}

module.exports.groupByKey = function(grid, node, worker) {		// REWRITE
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

	this.tx_shuffle = function(state) {
		var map = worker.map(function() {return {};});
		for (var p in this.result)
			map[ml.cksum(p) % worker.length][p] = this.result[p];
		for (var i = 0; i < map.length; i++)
			if (grid.host.uuid == worker[i].uuid) this.rx_shuffle(map[i], state);
			else grid.request(worker[i], {cmd: 'shuffle', args: map[i]}, function(err) {if (err) throw new Error(err);});
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

module.exports.reduceByKey = function(grid, node, worker) {	// REWRITE
	this.result = {};
	var reducer = node.src;
	var initVal = node.args[0];
	var SRAM = this.SRAM = [];

	var map = worker.map(function() {return {};});

	this.pipeline = function(array) {
		var dest = this.result;
		for (var i = 0; i < array.length; i++) {
			var key = array[i][0];
			if (dest[key] == undefined)
				dest[key] = [[key, JSON.parse(JSON.stringify(initVal))]];
			dest[key][0][1] = reducer(dest[key][0][1], array[i][1]);
		}
	}

	this.tx_shuffle = function(state) {
		// var map = worker.map(function() {return {};});
		for (var key in this.result)
			map[ml.cksum(key) % worker.length][key] = this.result[key];
		for (var i = 0; i < map.length; i++)
			if (grid.host.uuid == worker[i].uuid) this.rx_shuffle(map[i], state);
			else grid.request(worker[i], {cmd: 'shuffle', args: map[i]}, function(err) {if (err) throw new Error(err);});
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

module.exports.join = function(grid, node, worker) {
	this.SRAM = [];
	var type = node.args[1];

	var tmp = {};
	tmp[node.child[0]] = {};
	tmp[node.child[1]] = {};
	var map = worker.map(function() {return {};});
	for (var i = 0; i < worker.length; i++) {
		map[i][node.child[0]] = {};
		map[i][node.child[1]] = {};		
	}

	this.pipeline = function(array, p, src_id) {
		for (var i = 0; i < array.length; i++) {
			var key = array[i][0];
			var str = JSON.stringify(key);
			var wid = ml.cksum(str) % worker.length;
			if (map[wid][src_id][key] == undefined)
				map[wid][src_id][key] = [];
			map[wid][src_id][key].push(array[i][1]);
		}
	}

	this.tx_shuffle = function(state) {
		for (var i = 0; i < map.length; i++)
			if (grid.host.uuid == worker[i].uuid) this.rx_shuffle(map[i], state);
			else grid.request(worker[i], {cmd: 'shuffle', args: map[i]}, function(err) {if (err) throw new Error(err);});
	}

	this.rx_shuffle = function(data, state) {
		for (var i in data) {
			if (tmp[i] == undefined) {
				tmp[i] = data[i];
				continue;
			}
			for (var key in data[i]) {
				if (tmp[i][key] == undefined) {
					tmp[i][key] = data[i][key];
					continue;
				}
				tmp[i][key] = tmp[i][key].concat(data[i][key]);
			}
		}
		if (++state.nShuffle < worker.length) return;
		var key0 = Object.keys(tmp[node.child[0]]);
		var key1 = Object.keys(tmp[node.child[1]]);

		switch (type) {
		case 'inner':
			for (var i = 0; i < key0.length; i++) {
				if (key1.indexOf(key0[i]) == -1) continue;
				var data = [];
				for (var j = 0; j < tmp[node.child[0]][key0[i]].length; j++)
					for (var k = 0; k < tmp[node.child[1]][key0[i]].length; k++)
						data.push([JSON.parse(key0[i]), [tmp[node.child[0]][key0[i]][j], tmp[node.child[1]][key0[i]][k]]])
				this.SRAM.push({key: key0[i], data: data});
			}
			break;
		case 'left':
			for (var i = 0; i < key0.length; i++) {
				if (key1.indexOf(key0[i]) == -1) {
					var data = [];
					for (var j = 0; j < tmp[node.child[0]][key0[i]].length; j++)
						data.push([JSON.parse(key0[i]), [tmp[node.child[0]][key0[i]][j], undefined]])					
				} else {
					var data = [];
					for (var j = 0; j < tmp[node.child[0]][key0[i]].length; j++)
						for (var k = 0; k < tmp[node.child[1]][key0[i]].length; k++)
							data.push([JSON.parse(key0[i]), [tmp[node.child[0]][key0[i]][j], tmp[node.child[1]][key0[i]][k]]])
				}
				this.SRAM.push({key: key0[i], data: data});				
			}
			break;
		case 'right':
			for (var i = 0; i < key1.length; i++) {
				if (key0.indexOf(key1[i]) == -1) {
					var data = [];
					for (var j = 0; j < tmp[node.child[1]][key1[i]].length; j++)
						data.push([JSON.parse(key1[i]), [undefined, tmp[node.child[1]][key1[i]][j]]])					
				} else {
					var data = [];
					for (var j = 0; j < tmp[node.child[0]][key1[i]].length; j++)
						for (var k = 0; k < tmp[node.child[1]][key1[i]].length; k++)
							data.push([JSON.parse(key1[i]), [tmp[node.child[0]][key1[i]][j], tmp[node.child[1]][key1[i]][k]]])
				}
				this.SRAM.push({key: key1[i], data: data});				
			}
			break;			
		}
	}
}

module.exports.coGroup = function(grid, node, worker) {		// REWRITE
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

	this.tx_shuffle = function(state) {
		var map = worker.map(function() {return {};});
		for (var p in this.result)
			map[ml.cksum(p) % worker.length][p] = this.result[p];
		for (var i = 0; i < map.length; i++)
			if (grid.host.uuid == worker[i].uuid) this.rx_shuffle(map[i], state);
			else grid.request(worker[i], {cmd: 'shuffle', args: map[i]}, function(err) {if (err) throw new Error(err);});
	}

	this.rx_shuffle = function(data, state) {
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

module.exports.crossProduct = function(grid, node, worker) {
	this.SRAM = [];
	var left_dataset = node.child[0];
	var residentPartitions = [], shuffledPartitions = [];
	var map = worker.map(function() {return shuffledPartitions;});	
	var rxPartitions = [];

	this.pipeline = function(array, p, src_id) {
		var dest = (src_id == left_dataset) ? residentPartitions : shuffledPartitions;
		if (dest[p] == undefined) dest[p] = [];
		for (var i = 0; i < array.length; i++)
			dest[p].push(array[i]);		
	}

	this.tx_shuffle = function(state) {
		for (var i = 0; i < map.length; i++)
			if (grid.host.uuid == worker[i].uuid) this.rx_shuffle(map[i], state);
			else grid.request(worker[i], {cmd: 'shuffle', args: map[i]}, function(err) {if (err) throw new Error(err);});
	}

	function crossProduct(a, b) {
		var t0 = [];
		for (var i = 0; i < a.length; i++)
			for (var j = 0; j < b.length; j++)
				t0.push([a[i], b[j]]);
		return t0;
	}

	this.rx_shuffle = function(data, state) {
		for (var i = 0; i < data.length; i++)
			rxPartitions.push(data[i]);

		if (++state.nShuffle < worker.length) return;		
		for (var j = 0; j < rxPartitions.length; j++) {
			// var data = [];
			for (var i = 0; i < residentPartitions.length; i++)
				this.SRAM.push({data: crossProduct(residentPartitions[i], rxPartitions[j])});
			// this.SRAM.push({data: data});
		}
	}
}

module.exports.distinct = function(grid, node, worker) {
	this.SRAM = [];

	var tmp = [];
	var map = worker.map(function() {return [];});

	this.pipeline = function(array, p, src_id) {
		for (var i = 0; i < array.length; i++) {
			var str = JSON.stringify(array[i]);
			var wid = ml.cksum(str) % worker.length;
			if (map[wid].indexOf(str) == -1)
				map[wid].push(str);
		}
	}

	this.tx_shuffle = function(state) {
		for (var i = 0; i < map.length; i++)
			if (grid.host.uuid == worker[i].uuid) this.rx_shuffle(map[i], state);
			else grid.request(worker[i], {cmd: 'shuffle', args: map[i]}, function(err) {if (err) throw new Error(err);});
	}

	this.rx_shuffle = function (data, state) {
		for (var i = 0; i < data.length; i++)
			if (tmp.indexOf(data[i]) == -1) tmp.push(data[i]);
		if (++state.nShuffle < worker.length) return;
		this.SRAM = [{data: tmp.map(JSON.parse)}];
	}
}

module.exports.intersection = function(grid, node, worker) {
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

	this.tx_shuffle = function(state) {
		for (var i = 0; i < map.length; i++)
			if (grid.host.uuid == worker[i].uuid) this.rx_shuffle(map[i], state);
			else grid.request(worker[i], {cmd: 'shuffle', args: map[i]}, function(err) {if (err) throw new Error(err);});
	}

	this.rx_shuffle = function(data, state) {
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

module.exports.subtract = function(grid, node, worker) {
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

	this.tx_shuffle = function(state) {
		for (var i = 0; i < map.length; i++)
			if (grid.host.uuid == worker[i].uuid) this.rx_shuffle(map[i], state);
			else grid.request(worker[i], {cmd: 'shuffle', args: map[i]}, function(err) {if (err) throw new Error(err);});
	}

	this.rx_shuffle = function(data, state) {
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

module.exports.flatMapValues = function(grid, node) {
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

module.exports.partitionByKey = function(grid, node, worker) {
	this.SRAM = [];

	var tmp = {};
	var map = worker.map(function() {return {};});

	this.pipeline = function(array, p, src_id) {
		for (var i = 0; i < array.length; i++) {
			var str = JSON.stringify(array[i][0]);
			var wid = ml.cksum(str) % worker.length;
			if (map[wid][str] == undefined)
				map[wid][str] = [];
			map[wid][str].push(array[i]);
		}
	}

	this.tx_shuffle = function(state) {
		for (var i = 0; i < map.length; i++)
			if (grid.host.uuid == worker[i].uuid) this.rx_shuffle(map[i], state);
			else grid.request(worker[i], {cmd: 'shuffle', args: map[i]}, function(err) {if (err) throw new Error(err);});
	}

	this.rx_shuffle = function (data, state) {
		for (var key in data) {
			if (tmp[key] == undefined) tmp[key] = data[key]
			else tmp[key] = tmp[key].concat(data[key]);
		}
		if (++state.nShuffle < worker.length) return;
		for (var key in tmp)
			this.SRAM.push({data: tmp[key], key: JSON.parse(key)});
	}
}

module.exports.sortByKey = function(grid, node, worker) {
	this.SRAM = [];

	var tmp = {}, tmp2 = {}, keys = [];
	var map = worker.map(function() {return tmp;});

	function split(a, n) {
		var len = a.length, out = [], i = 0;
		while (i < len) {
			var size = Math.ceil((len - i) / n--);
			out.push(a.slice(i, i += size))
		}
		return out;
	}

	this.pipeline = function(array, p, src_id) {
		for (var i = 0; i < array.length; i++) {
			if (tmp[array[i][0]] == undefined)
				tmp[array[i][0]] = {key: array[i][0], data: []};
			tmp[array[i][0]].data.push(array[i][1]);
		}
	}

	this.tx_shuffle = function(state) {
		for (var i = 0; i < map.length; i++)
			if (grid.host.uuid == worker[i].uuid) this.rx_shuffle(map[i], state);
			else grid.request(worker[i], {cmd: 'shuffle', args: map[i]}, function(err) {if (err) throw new Error(err);});
	}

	this.rx_shuffle = function (data, state) {
		for (var key in data) {
			if (tmp2[key] == undefined) {
				tmp2[key] = data[key];
				keys.push(key);
				keys.sort();
			} else tmp2[key] = tmp2[key].concat(data[key]);
		}
		if (++state.nShuffle < worker.length) return;
		// Compute partition mapping over workers
		var mapping = split(keys, worker.length);
		for (var i = 0; i < mapping.length; i++) {
			if (worker[i].uuid != grid.host.uuid) continue;
			for (var j = 0; j < mapping[i].length; j++)
				this.SRAM.push({key: tmp2[mapping[i][j]].key, data: tmp2[mapping[i][j]].data.map(function(e) {
					return [tmp2[mapping[i][j]].key, e];
				})});
		}
	}
}
