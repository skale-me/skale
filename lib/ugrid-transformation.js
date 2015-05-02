'use strict';

var ml = require('./ugrid-ml.js');

function recompile(s) {
	var args = s.match(/\(([^)]*)/)[1];
	var body = s.replace(/^function *[^)]*\) *{/, '').replace(/}$/, '');
	return new Function(args, body);
}

function Action(grid, app, job, action) {
	if (action.src) {
		action.ml = ml;
		action.src = recompile(action.src);
	}
	this.semaphore = 0;

	var stream = grid.createWriteStream(job.id, app.master_uuid);

	this.sendResult = function() {
		stream.write(this.result);
	}
}

// ------------------------------------------------------------------------------------ //
// Actions
// ------------------------------------------------------------------------------------ //
module.exports.takeOrdered = function(grid, app, job, action) {
	Action.call(this, grid, app, job, action);
	var num = action.args[0];
	var sorter = action.args[1];
	this.result = [];

	this.reset = function() {
		this.result = [];
	};

	this.pipeline = function(array) {
		for (var i = 0; i < array.length; i++) {
			this.result.push(array[i]);
			this.result = this.result.sort(sorter);
			this.result = this.result.slice(0, num);
		}
	};
};

module.exports.top = function(grid, app, job, action) {
	Action.call(this, grid, app, job, action);
	var num = action.args[0];
	var sorter = action.args[1];
	this.result = [];

	this.reset = function() {
		this.result = [];
	};

	this.pipeline = function(array) {
		for (var i = 0; i < array.length; i++) {
			this.result.push(array[i]);
			this.result = this.result.sort(sorter);
			this.result = this.result.slice(0, num);
		}
	};
};

module.exports.take = function(grid, app, job, action) {
	Action.call(this, grid, app, job, action);	
	this.result = [];
	var num = action.args[0];

	this.reset = function() {
		this.result = [];
	};

	this.pipeline = function(array) {
		for (var i = 0; i < array.length; i++)
			if (this.result.length < num) this.result.push(array);
			else break;
	};
};

module.exports.count = function(grid, app, job, action) {
	Action.call(this, grid, app, job, action);
	this.result = 0;

	this.reset = function() {
		this.result = 0;
	};

	this.pipeline = function(array) {
		this.result += array.length;
	};
};

module.exports.collect = function(grid, app, job, action) {
	Action.call(this, grid, app, job, action);
	this.result = {};

	this.reset = function() {
		this.result = {};
	};

	this.pipeline = function(array, p, head) {
		if (this.result[p] === undefined) this.result[p] = [];
		var i, dest = this.result[p];
		if (head)
			for (i = array.length - 1; i >= 0; i--) dest.unshift(array[i]);
		else
			for (i = 0; i < array.length; i++) dest.push(array[i]);
	};
};

module.exports.reduce = function(grid, app, job, action) {
	Action.call(this, grid, app, job, action);
	this.result = JSON.parse(JSON.stringify(action.args[0]));
	var reduce = action.src;

	this.reset = function() {
		this.result = JSON.parse(JSON.stringify(action.args[0]));
	};

	this.pipeline = function(array) {
		for (var i = 0; i < array.length; i++)
			this.result = reduce(this.result, array[i]);
	};
};

module.exports.lookup = function(grid, app, job, action) {
	Action.call(this, grid, app, job, action);
	this.result = {};
	var key = action.args[0];

	this.reset = function() {
		this.result = {};
	};

	this.pipeline = function (array, p) {
		if (this.result[p] === undefined) this.result[p] = [];
		var dest = this.result[p];
		for (var i = 0; i < array.length; i++)
			if (array[i][0] == key) dest.push(array[i]);
	};
};

module.exports.countByValue = function(grid, app, job, action) {
	Action.call(this, grid, app, job, action);
	this.result = {};

	this.reset = function() {
		this.result = {};
	};

	this.pipeline = function(array) {
		for (var i = 0; i < array.length; i++) {
			var str = JSON.stringify(array[i]);
			if (this.result[str] === undefined)
				this.result[str] = [array[i], 0];
			this.result[str][1]++;
		}
	};
};

module.exports.forEach = function(grid, app, job, action) {
	Action.call(this, grid, app, job, action);
	var each = action.src;

	this.reset = function() {};

	this.pipeline = function(array) {
		array.forEach(each);
	};
};

// ------------------------------------------------------------------------------------ //
// Transformations
// ------------------------------------------------------------------------------------ //
function Transform(grid, app, job, stage, node) {
	if (node.src) node.src = recompile(node.src);

	this.tx_shuffle = function() {
		for (var i = 0; i < this.map.length; i++)
			if (grid.host.uuid == app.worker[i].uuid) this.rx_shuffle(this.map[i], stage);
			else grid.send(app.worker[i].uuid, {cmd: 'shuffle', args: this.map[i], jobId: job.id, sid: stage.sid});
	};
}

module.exports.union = function(grid, app, job, stage, node) {
	Transform.call(this, grid, app, job, stage, node);
	this.pipeline = function (array) {return array;};
};

module.exports.map = function(grid, app, job, stage, node) {
	Transform.call(this, grid, app, job, stage, node);
	var mapper = node.src;
	var args = node.args;

	this.pipeline = function(array) {
		for (var i = 0; i < array.length; i++)
			array[i] = mapper(array[i], args[0]);
		return array;
	};
};

module.exports.filter = function(grid, app, job, stage, node) {
	Transform.call(this, grid, app, job, stage, node);
	var filter = node.src;

	this.pipeline = function(array) {
		var tmp = [];
		for (var i = 0; i < array.length; i++)
			if (filter(array[i])) tmp.push(array[i]);
		return tmp;
	};
};

module.exports.flatMap = function(grid, app, job, stage, node) {
	Transform.call(this, grid, app, job, stage, node);	
	var mapper = node.src;

	this.pipeline = function(array) {
		var tmp = [];
		for (var i = 0; i < array.length; i++)
			tmp = tmp.concat(mapper(array[i]));
		return tmp;
	};
};

module.exports.mapValues = function(grid, app, job, stage, node) {
	Transform.call(this, grid, app, job, stage, node);
	var mapper = node.src;

	this.pipeline = function(array) {
		for (var i = 0; i < array.length; i++)
			array[i][1] = mapper(array[i][1]);
		return array;
	};
};

module.exports.flatMapValues = function(grid, app, job, stage, node) {
	Transform.call(this, grid, app, job, stage, node);
	var mapper = node.src;

	this.pipeline = function(array) {
		var tmp = [];
		for (var i = 0; i < array.length; i++) {
			var t0 = mapper(array[i][1]);
			tmp = tmp.concat(t0.map(function(e) {return [array[i][0], e];}));
		}
		return tmp;
	};
};

module.exports.sample = function(grid, app, job, stage, node) {
	Transform.call(this, grid, app, job, stage, node);	
	var withReplacement = node.args[0];
	var frac = node.args[1];
	var seed = node.args[2];
	this.SRAM = [];
	var tmp = {};

	this.pipeline = function(array, p) {
		if (tmp[p] === undefined) tmp[p] = [];
		for (var i = 0; i < array.length; i++)
			tmp[p].push(array[i]);
	};

	this.tx_shuffle = function() {
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
		stage.nShuffle = app.worker.length;	// Cancel shuffle
	};
};

module.exports.groupByKey = function(grid, app, job, stage, node) {
	Transform.call(this, grid, app, job, stage, node);
	var SRAM = this.SRAM = [];
	var map = this.map = app.worker.map(function() {return {};});

	this.pipeline = function(array) {
		for (var i = 0; i < array.length; i++) {
			var key = array[i][0];
			var wid = ml.cksum(key) % app.worker.length;
			if (map[wid][key] === undefined) map[wid][key] = [[key, []]];
			map[wid][key][0][1].push(array[i][1]);
		}
	};

	this.rx_shuffle = function(data) {
		for (var key in data) {
			for (var i = 0; i < SRAM.length; i++)
				if (SRAM[i].key == key) break;
			if (i == SRAM.length) SRAM.push({key: key, data: data[key]});
			else SRAM[i].data[0][1] = SRAM[i].data[0][1].concat(data[key][0][1]);
		}
		stage.nShuffle++;
	};
};

module.exports.reduceByKey = function(grid, app, job, stage, node) {
	Transform.call(this, grid, app, job, stage, node);
	var reducer = node.src;
	var initVal = node.args[0];
	var SRAM = this.SRAM = [];
	var map = this.map = app.worker.map(function() {return {};});

	this.pipeline = function(array) {
		for (var i = 0; i < array.length; i++) {
			var key = array[i][0];
			var wid = ml.cksum(key) % app.worker.length;
			if (map[wid][key] === undefined)
				map[wid][key] = [[key, JSON.parse(JSON.stringify(initVal))]];
			map[wid][key][0][1] = reducer(map[wid][key][0][1], array[i][1]);
		}
	};

	this.rx_shuffle = function(data) {
		for (var key in data) {
			for (var i = 0; i < SRAM.length; i++)
				if (SRAM[i].key == key) break;
			if (i == SRAM.length) SRAM.push({key: key, data: data[key]});
			else SRAM[i].data[0][1] = reducer(SRAM[i].data[0][1], data[key][0][1]);
		}
		stage.nShuffle++;
	};
};

module.exports.join = function(grid, app, job, stage, node) {
	Transform.call(this, grid, app, job, stage, node);	
	this.SRAM = [];
	var type = node.args[1];
	var tmp = {};
	tmp[node.child[0]] = {};
	tmp[node.child[1]] = {};
	var map = this.map = app.worker.map(function() {return {};});
	for (var i = 0; i < app.worker.length; i++) {
		map[i][node.child[0]] = {};
		map[i][node.child[1]] = {};
	}

	this.pipeline = function(array, p, from_id) {
		for (var i = 0; i < array.length; i++) {
			var key = array[i][0];
			var str = JSON.stringify(key);
			var wid = ml.cksum(str) % app.worker.length;
			if (map[wid][from_id][key] === undefined)
				map[wid][from_id][key] = [];
			map[wid][from_id][key].push(array[i][1]);
		}
	};

	this.rx_shuffle = function(data) {
		var i, j, k;
		for (i in data) {
			if (tmp[i] === undefined) {
				tmp[i] = data[i];
				continue;
			}
			for (var key in data[i]) {
				if (tmp[i][key] === undefined) {
					tmp[i][key] = data[i][key];
					continue;
				}
				tmp[i][key] = tmp[i][key].concat(data[i][key]);
			}
		}
		if (++stage.nShuffle < app.worker.length) return;
		var key0 = Object.keys(tmp[node.child[0]]);
		var key1 = Object.keys(tmp[node.child[1]]);

		switch (type) {
		case 'inner':
			for (i = 0; i < key0.length; i++) {
				if (key1.indexOf(key0[i]) == -1) continue;
				data = [];
				for (j = 0; j < tmp[node.child[0]][key0[i]].length; j++)
					for (k = 0; k < tmp[node.child[1]][key0[i]].length; k++)
						data.push([JSON.parse(key0[i]), [tmp[node.child[0]][key0[i]][j], tmp[node.child[1]][key0[i]][k]]]);
				this.SRAM.push({key: key0[i], data: data});
			}
			break;
		case 'left':
			for (i = 0; i < key0.length; i++) {
				if (key1.indexOf(key0[i]) == -1) {
					data = [];
					for (j = 0; j < tmp[node.child[0]][key0[i]].length; j++)
						data.push([JSON.parse(key0[i]), [tmp[node.child[0]][key0[i]][j], null]]);
				} else {
					data = [];
					for (j = 0; j < tmp[node.child[0]][key0[i]].length; j++)
						for (k = 0; k < tmp[node.child[1]][key0[i]].length; k++)
							data.push([JSON.parse(key0[i]), [tmp[node.child[0]][key0[i]][j], tmp[node.child[1]][key0[i]][k]]]);
				}
				this.SRAM.push({key: key0[i], data: data});
			}
			break;
		case 'right':
			for (i = 0; i < key1.length; i++) {
				if (key0.indexOf(key1[i]) == -1) {
					data = [];
					for (j = 0; j < tmp[node.child[1]][key1[i]].length; j++)
						data.push([JSON.parse(key1[i]), [null, tmp[node.child[1]][key1[i]][j]]]);
				} else {
					data = [];
					for (j = 0; j < tmp[node.child[0]][key1[i]].length; j++)
						for (k = 0; k < tmp[node.child[1]][key1[i]].length; k++)
							data.push([JSON.parse(key1[i]), [tmp[node.child[0]][key1[i]][j], tmp[node.child[1]][key1[i]][k]]]);
				}
				this.SRAM.push({key: key1[i], data: data});
			}
			break;
		}
	};
};

module.exports.coGroup = function(grid, app, job, stage, node) {
	Transform.call(this, grid, app, job, stage, node);
	var SRAM = this.SRAM = {};
	var map = this.map = app.worker.map(function() {return {};});

	this.pipeline = function(array, p, from_id) {
		for (var i = 0; i < array.length; i++) {
			var key = array[i][0];
			var wid = ml.cksum(key) % app.worker.length;
			if (map[wid][key] === undefined) map[wid][key] = {key: key};
			if (map[wid][key][from_id] === undefined) map[wid][key][from_id] = [];
			map[wid][key][from_id].push(array[i][1]);
		}
	};

	this.rx_shuffle = function(data) {
		var i, k;
		for (i in data)
			if (i in SRAM) {
				for (k in data[i]) {
					if (k == 'key') continue;
					if (SRAM[i][k] === undefined) SRAM[i][k] = data[i][k];
					else SRAM[i][k] = SRAM[i][k].concat(data[i][k]);
				}
			} else SRAM[i] = data[i];
		// When all shuffle has been received
		if (++stage.nShuffle < app.worker.length) return;
		var res = [];
		for (i in SRAM) {
			var datasets = Object.keys(SRAM[i]);
			if (datasets.length != 3) continue;
			res.push({
				key: SRAM[i].key,
				data:[[SRAM[i].key, [SRAM[i][node.child[0]], SRAM[i][node.child[1]]]]]
			});
		}
		this.SRAM = res;
	};
};

module.exports.crossProduct = function(grid, app, job, stage, node) {
	Transform.call(this, grid, app, job, stage, node);
	this.SRAM = [];
	var left_dataset = node.child[0];
	var residentPartitions = [], shuffledPartitions = [];
	var map = this.map = app.worker.map(function() {return shuffledPartitions;});
	var rxPartitions = [];

	this.pipeline = function(array, p, from_id) {
		var dest = (from_id == left_dataset) ? residentPartitions : shuffledPartitions;
		if (dest[p] === undefined) dest[p] = [];
		for (var i = 0; i < array.length; i++)
			dest[p].push(array[i]);
	};

	function crossProduct(a, b) {
		var t0 = [];
		for (var i = 0; i < a.length; i++)
			for (var j = 0; j < b.length; j++)
				t0.push([a[i], b[j]]);
		return t0;
	}

	this.rx_shuffle = function(data) {
		var i, j;
		for (i = 0; i < data.length; i++)
			rxPartitions.push(data[i]);

		if (++stage.nShuffle < app.worker.length) return;
		for (j = 0; j < rxPartitions.length; j++) {
			for (i = 0; i < residentPartitions.length; i++)
				this.SRAM.push({data: crossProduct(residentPartitions[i], rxPartitions[j])});
		}
	};
};

module.exports.distinct = function(grid, app, job, stage, node) {
	Transform.call(this, grid, app, job, stage, node);
	this.SRAM = [];
	var tmp = [];
	var map = this.map = app.worker.map(function() {return [];});

	this.pipeline = function(array) {
		for (var i = 0; i < array.length; i++) {
			var str = JSON.stringify(array[i]);
			var wid = ml.cksum(str) % app.worker.length;
			if (map[wid].indexOf(str) == -1) map[wid].push(str);
		}
	};

	this.rx_shuffle = function (data) {
		for (var i = 0; i < data.length; i++)
			if (tmp.indexOf(data[i]) == -1) tmp.push(data[i]);
		if (++stage.nShuffle < app.worker.length) return;
		this.SRAM = [{data: tmp.map(JSON.parse)}];
	};
};

module.exports.intersection = function(grid, app, job, stage, node) {
	Transform.call(this, grid, app, job, stage, node);
	this.SRAM = [];
	var tmp = {};
	tmp[node.child[0]] = [];
	tmp[node.child[1]] = [];
	var map = this.map = app.worker.map(function() {return {};});
	for (var i = 0; i < app.worker.length; i++) {
		map[i][node.child[0]] = [];
		map[i][node.child[1]] = [];
	}

	this.pipeline = function(array, p, from_id) {
		for (var i = 0; i < array.length; i++) {
			var str = JSON.stringify(array[i]);
			var wid = ml.cksum(str) % app.worker.length;
			map[wid][from_id].push(str);
		}
	};

	this.rx_shuffle = function(data) {
		var i, j, k;
		for (i in data) tmp[i].push(data[i]);
		if (++stage.nShuffle < app.worker.length) return;
		var result = [];
		for (i = 0; i < tmp[node.child[0]].length; i++)
			loop:
			for (j = 0; j < tmp[node.child[0]][i].length; j++) {
				var e = tmp[node.child[0]][i][j];
				if (result.indexOf(e) != -1) continue;
				// Rechercher e dans toutes les partitions de child[1]
				for (k = 0; k < tmp[node.child[1]].length; k++)
					if (tmp[node.child[1]][k].indexOf(e) != -1) {
						result.push(e);
						continue loop;
					}
			}
		this.SRAM = [{data: []}];
		for (i = 0; i < result.length; i++)
			this.SRAM[0].data.push(JSON.parse(result[i]));
	};
};

module.exports.subtract = function(grid, app, job, stage, node) {
	Transform.call(this, grid, app, job, stage, node);
	this.SRAM = [];
	var tmp = {};
	tmp[node.child[0]] = [];
	tmp[node.child[1]] = [];
	var map = this.map = app.worker.map(function() {return {};});
	for (var i = 0; i < app.worker.length; i++) {
		map[i][node.child[0]] = [];
		map[i][node.child[1]] = [];
	}

	this.pipeline = function(array, p, from_id) {
		for (var i = 0; i < array.length; i++) {
			var str = JSON.stringify(array[i]);
			var wid = ml.cksum(str) % app.worker.length;
			map[wid][from_id].push(str);
		}
	};

	this.rx_shuffle = function(data) {
		var i;
		for (i in data) tmp[i] = tmp[i].concat(data[i]);
		if (++stage.nShuffle < app.worker.length) return;
		var v1 = tmp[node.child[0]];
		var v2 = tmp[node.child[1]];
		var v_ref = [];
		for (i = 0; i < v1.length; i++) {
			if (v_ref.indexOf(v1[i]) != -1) continue;
			if (v2.indexOf(v1[i]) != -1) continue;
			v_ref.push(v1[i]);
		}
		this.SRAM = [{data: v_ref.map(JSON.parse)}];
	};
};

module.exports.partitionByKey = function(grid, app, job, stage, node) {
	Transform.call(this, grid, app, job, stage, node);
	this.SRAM = [];
	var tmp = {};
	var map = this.map = app.worker.map(function() {return {};});

	this.pipeline = function(array) {
		for (var i = 0; i < array.length; i++) {
			var str = JSON.stringify(array[i][0]);
			var wid = ml.cksum(str) % app.worker.length;
			if (map[wid][str] === undefined)
				map[wid][str] = [];
			map[wid][str].push(array[i]);
		}
	};

	this.rx_shuffle = function (data) {
		var key;
		for (key in data) {
			if (tmp[key] === undefined) tmp[key] = data[key];
			else tmp[key] = tmp[key].concat(data[key]);
		}
		if (++stage.nShuffle < app.worker.length) return;
		for (key in tmp)
			this.SRAM.push({data: tmp[key], key: JSON.parse(key)});
	};
};

module.exports.sortByKey = function(grid, app, job, stage, node) {
	Transform.call(this, grid, app, job, stage, node);
	this.SRAM = [];
	var tmp = {}, tmp2 = {}, keys = [];
	var map = this.map = app.worker.map(function() {return tmp;});

	function split(a, n) {
		var len = a.length, out = [], i = 0;
		while (i < len) {
			var size = Math.ceil((len - i) / n--);
			out.push(a.slice(i, i += size));
		}
		return out;
	}

	this.pipeline = function(array) {
		for (var i = 0; i < array.length; i++) {
			if (tmp[array[i][0]] === undefined)
				tmp[array[i][0]] = {key: array[i][0], data: []};
			tmp[array[i][0]].data.push(array[i][1]);
		}
	};

	this.rx_shuffle = function (data) {
		for (var key in data) {
			if (tmp2[key] === undefined) {
				tmp2[key] = data[key];
				keys.push(key);
				keys.sort();
			} else tmp2[key] = tmp2[key].concat(data[key]);
		}
		if (++stage.nShuffle < app.worker.length) return;
		// Compute partition mapping over workers
		var mapping = split(keys, app.worker.length);
		for (var i = 0; i < mapping.length; i++) {
			if (app.worker[i].uuid != grid.host.uuid) continue;
			for (var j = 0; j < mapping[i].length; j++)
				this.SRAM.push({key: tmp2[mapping[i][j]].key, data: tmp2[mapping[i][j]].data.map(function(e) {
					return [tmp2[mapping[i][j]].key, e];
				})});
		}
	};
};
