/*
	TODO:
		- loadTextFile
		- parrallelize		
		- filter
		- flatMap
		- sample
		- lookup
		- reduceByKey
		- takeSample
		- gérer un nombre variable d'arguments pour les fonctions utilisateurs (reduce)
		- renforcer la génération des id des arrays		
		- parallelize ne fonctionne que pour un seul worker
		  il faut découper le vecteur avec du hashcoding avant d'envoyer la tache

	NB:
		- attention lors de la génération des lineage lorsque la data est déjà en RAM
		  il se peut que l'on calcul deux fois la meme donnée car deux lineages peuvent partir
		  de la meme partition déjà en RAM
		- prendre en argument la STAGE_INPUT_RAM dans les stage d'indice > 1
		- faire une structure de donnée stoackant les différents runStage
		  et la logique d'enchainement des stages côté worker
*/

var thunkify = require('thunkify');
var ml = require('./ugrid-ml.js');

function UgridArray(grid, worker) {
	function rpc(grid, uuid, args, callback) {
		var payload = {cmd: "task", args: args};
		grid.send_cb('request', {uuid: uuid, payload: payload}, callback);
	}

	this.id = Math.round(Math.random() * 1e9);			// Unique array id
	this.inMemory = false;
	this.anc;
	this.child = [];
	this.dependency = [];
	this.persistent = false;
	this.visits = 0;
	this.lineageArray = [];
	this.transform;
	this.transformType;	

	this.setDependency = function setDependency(child, transform, args) {
		this.transform = transform;
		switch (transform) {
		case 'loadTestData':
		case 'parallelize':
		case 'map':
		case 'union':
			this.transformType = 'narrow';
			break;
		case 'reduceByKey':
			this.transformType = 'wide';
			break;
		default:
			throw 'setDependency error: Transform ' + transform + ' type not yet known'
		}
		for (var i = 0; i < child.length; i++) {
			this.dependency.push({srcGridArrayId: child[i], args: args})
			if (child[i]) {
				this.child.push(child[i]);
				child[i].anc = this;
			}
		}
	};

	function buildTask(array, action) {
		var lineages = [];

		function treewalk(root, c_in, c_out) {
			var n = root;
			if (c_in) c_in(n);
			while (1) {
		        if ((n.child.length > 0) && (++n.visits <= n.child.length)) {
	                n = n.child[n.visits - 1];
	                if (c_in) c_in(n);
		        } else {
	                n.visits = 0;
	                if (c_out) c_out(n);
	                if (n == root) break;
	                n = n.anc;
		        }
		    }
		}

		// Step 1: build full lineage, ignoring in-memory attribute
		var nStage = 1;
		var nTotalStage = 1;
		treewalk(array, function(a) {
			a.lineageArray = [];
			if (a.transformType == 'wide')
				nStage++;
			if (nTotalStage < nStage)
				nTotalStage = nStage;
		}, function(a) {
			if (a.child.length == 0) {
				lineages.push([]);
				var idx = lineages.length - 1;
				a.lineageArray.push(idx);
				lineages[idx].push({
					srcId: null,
					destId: a.id,
					persistent: a.persistent,
					inMemory: a.inMemory,
					stage: nStage,
					transform: a.transform,
					args: a.dependency[0].args
				});
			} else {
				for (var i = 0; i < a.child.length; i++) {
					var t = {
						srcId: a.dependency[i].srcGridArrayId.id,
						destId: a.id,
						persistent: a.persistent,
						inMemory: a.inMemory,
						stage: nStage,
						transform: a.transform,
						args: a.dependency[i].args
					}
					for (var j = 0; j < a.child[i].lineageArray.length; j++)
						lineages[a.child[i].lineageArray[j]].push(t);
					a.lineageArray = a.lineageArray.concat(a.child[i].lineageArray)
				}
			}
			if (a.persistent)
				a.inMemory = true;
			if (a.transformType == 'wide')
				nStage--;			
		});

		// Step 2: for each lineage find last in-memory array and remove previous transforms from lineage
		for (var i = 0; i < lineages.length; i++) {
			var spliceIdx = 0;
			for (var j = 0; j < lineages[i].length; j++) {
				lineages[i][j].stage -= nTotalStage - 1;
				if (lineages[i][j].inMemory)
					spliceIdx = j + 1;
			}
			lineages[i].splice(0, spliceIdx);
		}

		// XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX //
		// Step 2-bis: supprimer les lineages an double liés  la construction
		// si le point de départ de deux lineages est le meme srcId, en conserver un seul
		// solution numéro 2, préempter le push des transfo dans la remontée du trewalk si 
		// on se trouve sous un ancetre en RAM
		// XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX //

		// Step 3: Build task stages
		var stage = new Array(nTotalStage);
		for (var i = 0; i < nTotalStage; i++)
			stage[i] = [];
		for (var i = 0; i < lineages.length; i++) {
			for (var j = 0; j < lineages[i].length; j++) {
				if (j == 0) {
					stage[lineages[i][j].stage - 1].push([]);
					idx = stage[lineages[i][j].stage - 1].length - 1;
				}
				stage[lineages[i][j].stage - 1][idx].push(lineages[i][j]);
			}
		}
		if (stage[stage.length - 1].length == 0)
			stage[stage.length - 1].push([]);

		var stageStr = [];
		for (var i = 0; i < stage.length; i++) {
			if (i == (stage.length - 1))
				stageStr[i] = buildStage(stage[i], action);
			else
				stageStr[i] = buildStage(stage[i]);
		}

		// Write task constructor
		var lastStr = 'function task() {\n';
		lastStr += '\tthis.stageIdx = 0;\n';
		lastStr += '\tthis.run = [];\n';
		lastStr += '\tthis.worker = [];\n';
		for (var i = 0; i < worker.length; i++)
			lastStr += '\tthis.worker[' + i + '] = "' + worker[i] + '";\n';
		for (var i = 0; i < stageStr.length; i++)
			lastStr += '\tthis.run[' + i + '] = ' + stageStr[i] + ';\n';
		lastStr += '}\n';

		function buildStage(lineages, action) {
			var taskStr = "";
			var lastStage = action ? true : false;
			// Step 3: Generate task source
			if (lastStage) {
				var actionStr = {
					'reduce': {
						init:
							'var reducer = ' + action.args[0] + ';\n' +
							'var res = action.args[1];\n',
						run: '\t\tres = reducer(res, tmp);\n'
					},
					'count': {
						init: 'var res = 0;\n',
						run: '\t\tres++;\n'
					},
					'collect': {
						init: 'var res = [];\n',
						run: '\t\tres.push(tmp);\n'
					},
					'takeSample': {
						init: 'var res = [];\n',
						run:
							'\t\tres.push(tmp);\n' + 
							'\t\tif (res.length == action.args[0]) break;\n'
					}
				}
			}
			// Load action specific data if needed
			var alreadyPersistent = {};
			if (lastStage)
				taskStr += actionStr[action.fun].init;
			else 
				taskStr += 'var res = {};\n';
			taskStr += 'var transforms = {};\n';

			for (var l = 0; l < lineages.length; l++) {
				// -------------------------------------------------------------------------- //
				// action dataset already in RAM (run action immediately)
				// -------------------------------------------------------------------------- //
				if (lineages[l].length == 0) {
					taskStr += 'for (var p in RAM[action.srcId])\n';
					if (action.fun == 'reduce') {
						taskStr += 'for (var i = 0; i < RAM[action.srcId][p].length; i++)\n';
						taskStr += '\tres = reducer(res, RAM[action.srcId][p][i]);\n';			// Add user additional arguments to reducer here
					} else if (action.fun == 'count') {
						taskStr += 'res += RAM[action.srcId][p].length;';
					} else if (action.fun == 'collect') {
						taskStr += 'res = res.concat(RAM[action.srcId][p]);';
					}
					continue;
				}
				// -------------------------------------------------------------------------- //
				// Dataset has a lineages[0] to be computed (ie. not in RAM)
				// -------------------------------------------------------------------------- //
				if (lineages[l][0].srcId) {
					// First dataset in lineages[0] depends on another dataset already in RAM
					taskStr += 'var input = RAM[lineages[' + l + '][0].srcId];\n';
				} else {
					// First dataset depends on external ressources (file, function, etc.)
					if (lineages[l][0].transform == 'loadTestData')
						taskStr += 'var input = ml.loadTestData(lineages[' + l + '][0].args[0], lineages[' + l + '][0].args[1], lineages[' + l + '][0].args[2]);\n';
					else if (lineages[l][0].transform == 'parallelize') {
						taskStr += 'var input = lineages[' + l + '][0].args[0];\n';
					} else
						throw 'ugrid-array.js error: ' + lineages[l][0].transform + ' not supported yet'
					// make it persistent if needed
					if (lineages[l][0].persistent)
						taskStr += 'RAM[lineages[' + l + '][0].destId] = input;\n';
				}
				// -------------------------------------------------------------------------- //
				// Build transformations array and initialize resilient partition if needed
				// -------------------------------------------------------------------------- //
				taskStr += 'transforms[' + l + '] = {};\n';
				for (var i = 0; i < lineages[l].length; i++) {
					if ((lineages[l][i].transform == 'loadTestData') || (lineages[l][i].transform == 'parallelize'))
						continue;

					switch(lineages[l][i].transform) {
					case 'map': 
						taskStr += 'transforms[' + l + '][' + i + '] = ' + lineages[l][i].args[0] + ';\n';
						break;
					case 'union':
						break;
					case 'reduceByKey':
						taskStr += 'transforms[' + l + '][' + i + '] = ' + lineages[l][i].args[1] + ';\n';
						break;
					default:
						throw lineages[l][i].transform + ' error: transformation array entry not implemented'
					}

					if (lineages[l][i].persistent && (!alreadyPersistent[lineages[l][i].destId])) {
						alreadyPersistent[lineages[l][i].destId] = true;
						taskStr += 'RAM[lineages[' + l + '][' + i + '].destId] = [];\n';
						taskStr += 'for (var i = 0; i < input.length; i++) RAM[lineages[' + l + '][' + i + '].destId].push([]);\n';
					}
				}

				// -------------------------------------------------------------------------- //
				// Loop over input partitions and apply transformations pipeline
				// -------------------------------------------------------------------------- //
				taskStr += 'for (var p in input) {\n';
				taskStr += '\tfor (var i = 0; i < input[p].length; i++) {\n';
				taskStr += '\t\tvar tmp = input[p][i];\n';
				for (var i = 0; i < lineages[l].length; i++) {
					if (lineages[l][i].srcId == null)
						continue;

					// Modify behaviour here if different transform like filter, etc.
					switch(lineages[l][i].transform) {
					case 'map':
						var argStr = '';
						for (var arg = 0; arg < lineages[l][i].args[1].length; arg++)
							argStr += ', lineages[' + l + '][' + i + '].args[1][' + arg + ']';
						taskStr += '\t\ttmp = transforms[' + l + '][' + i + '](tmp' + argStr + ');\n';
						break;
					case 'union':
						break;
					case 'reduceByKey':
						// Créer une partition par clé
						taskStr += 'if (tmp[lineages[' + l + '][' + i + '].args[0]] == undefined) throw "key unknown by worker"\n';
						taskStr += 'if (res[tmp[lineages[' + l + '][' + i + '].args[0]]] == undefined) ' + 
								   'res[tmp[lineages[' + l + '][' + i + '].args[0]]] = ' + 
								   'JSON.parse(JSON.stringify(lineages[' + l + '][' + i + '].args[2]));';
						taskStr += 'res[tmp[lineages[' + l + '][' + i + '].args[0]]] = ' + 
								   		'transforms[' + l + '][' + i + '](res[tmp[lineages[' + l + '][' + i + '].args[0]]], tmp);\n';
						break;
					default:
						throw lineages[l][i].transform + ' error: pipeline call not implemented'
					}
					if ((lineages[l][i].persistent) && (lineages[l][i].transformType == 'narrow'))
						taskStr += '\t\tRAM[lineages[' + l + '][' + i + '].destId][p].push(tmp);\n';
				}
				// -------------------------------------------------------------------------- //
				// Run action at the end of transformations pipeline
				// -------------------------------------------------------------------------- //
				if (lastStage)
					taskStr += actionStr[action.fun].run;
				taskStr += '\t}\n}\n';
			}

			// Ici c'est la fin d'un stage, si c'est le dernier on retourne le résultat
			taskStr += 'return res;\n';
			taskStr = 'function runTask(ml, STAGE_RAM, RAM, lineages, action){\n' + taskStr + '}';
			// sinon il faut faire le shuffle dans la fonction récupérant le résultat
			return taskStr;
		}

		return {lineages: lineages, action: action, lastStr: lastStr}
	}

	this.persist = function() {
		this.persistent = true;
		return this;
	}

	this.map = function(mapper, mapperArgs) {
		var array = new UgridArray(grid, worker);
		var srcGridArrayId = this;
		var transform = 'map';
		var args = [mapper.toString(), mapperArgs];
		array.setDependency([srcGridArrayId], transform, args);
		return array;
	}

	this.union = function(withArray) {
		if (withArray.id == this.id)
			return this;
		var array = new UgridArray(grid, worker);
		var srcGridArrayId = this;
		var transform = 'union';
		var args = [withArray.id];
		array.setDependency([srcGridArrayId, withArray], transform, args);
		return array;
	}

	this.flatMap = function(mapper, mapperArgs) {
		throw 'this.flatMap not yet implemented'

		var array = new UgridArray(grid, worker);
		var srcGridArrayId = this;
		var transform = 'flatMap';
		var args = [mapper.toString(), mapperArgs];
		array.setDependency([srcGridArrayId], transform, args);
		return array;
	}

	this.filter = function(filter) {
		throw 'this.filter not yet implemented'

		var array = new UgridArray(grid, worker);
		var srcGridArrayId = this;
		var transform = 'filter';
		var args = [filter.toString()];
		array.setDependency([srcGridArrayId], transform, args);
		return array;
	}

	this.reduceByKey = function(key, reducer, initVal) {
		var array = new UgridArray(grid, worker);
		var srcGridArrayId = this;
		var transform = 'reduceByKey';
		var args = [key, reducer.toString(), initVal];
		array.setDependency([srcGridArrayId], transform, args);
		return array;
	}

	// Return the result of reduction function
	this.reduce_cb = function(reducer, aInit, callback) {
		var action = {srcId: this.id, fun: 'reduce', args: [reducer.toString(), aInit]}
		var task = buildTask(this, action);
		var nAnswer = 0, result = aInit;

		for (var i in worker)
			rpc(grid, worker[i], task, function(err, res) {
				result = reducer(result, res);
				if (++nAnswer == worker.length)
					callback(null, result);
			})
	}

	this.count_cb = function(callback) {
		var action = {srcId: this.id, fun: 'count', args: []}
		var task = buildTask(this, action);
		var nAnswer = 0, result = 0;

		for (var i in worker)
			rpc(grid, worker[i], task, function(err, res) {
				result += res;
				if (++nAnswer == worker.length)
					callback(null, result);
			})
	}

	// Return dataset entries
	this.collect_cb = function(callback) {
		var action = {srcId: this.id, fun: 'collect', args: []}
		var task = buildTask(this, action);
		var nAnswer = 0, result = [];

		for (var i in worker)
			rpc(grid, worker[i], task, function(err, res) {
				result = result.concat(res);
				if (++nAnswer == worker.length)
					callback(null, result);
			})
	}

	this.takeSample_cb = function(N, callback) {
		var action = {srcId: this.id, fun: 'takeSample', args: [N]}
		var task = buildTask(this, action);
		var nAnswer = 0, result = [];

		for (var i in worker)
			rpc(grid, worker[i], task, function(err, res) {
				result = result.concat(res);
				if (++nAnswer == worker.length) {
					if (result.length > N)
						result = result.splice(N, result.length);
					callback(null, result);
				}
			})
	}

	this.sample_cb = function(fraction, callback) {
		throw 'this.sample not yet implemented'

		var action = {srcId: this.id, fun: 'sample', args: [fraction]}
		var task = buildTask(this, action);
		var nAnswer = 0, result = [];

		for (var i in worker)
			rpc(grid, worker[i], task, function(err, res) {
				result = result.concat(res);
				if (++nAnswer == worker.length)
					callback(null, result);
			})
	}

	this.lookup_cb = function(key, callback) {
		throw 'this.lookup not yet implemented'

		var action = {srcId: this.id, fun: 'lookup', args: [key]}
		var task = buildTask(this, action);
		var nAnswer = 0, result = [];

		for (var i in worker)
			rpc(grid, worker[i], task, function(err, res) {
				result = result.concat(res);
				if (++nAnswer == worker.length)
					callback(null, result);
			})
	}

	this.reduce = thunkify(this.reduce_cb);
	this.count = thunkify(this.count_cb);
	this.collect = thunkify(this.collect_cb);
	this.takeSample = thunkify(this.takeSample_cb);
	this.sample = thunkify(this.sample_cb);
	this.lookup = thunkify(this.lookup_cb);
}

module.exports = UgridArray;