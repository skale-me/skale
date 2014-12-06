/*
TODO:
	- attention lors de la génération des lineage lorsque la data est déjà en RAM
	  il se peut que l'on calcul deux fois la meme donnée car deux lineages peuvent partir
	  de la meme partition déjà en RAM
	- pour chaque stage créer la commande à exécuter sur réception d'une request de shuffle
	- intégrer la STAGE_RAM comme input possible dans les pipelines et gérer la persistence

	- ATTENTION au Step 2: supprimer les lineages an double liés  la construction
	  si le point de départ de deux lineages est le meme srcId, en conserver un seul
	  solution numéro 2, préempter le push des transfo dans la remontée du trewalk si 
	  on se trouve sous un ancetre en RAM
*/

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

function Dependency(node, srcId, depIdx, nStage) {
	this.srcId = srcId,
	this.destId = node.id,
	this.persistent = node.persistent,
	this.inMemory = node.inMemory,
	this.stage = nStage,
	this.transform = node.transform,
	this.transformType = node.transformType,
	this.args =  node.dependency[depIdx].args
}

function buildTask(worker, array, action) {
	var lineages = [];
	var nStage = 0;
	var lastStage = 0;

	// ----------------------------------------------------------------------------------------- //
	// Step 1: build full lineage, ignoring in-memory attribute
	// ----------------------------------------------------------------------------------------- //	
	treewalk(array, function (a) {
		a.lineageArray = [];
		if (a.transformType == 'wide')
			nStage++;
		if (lastStage < nStage)
			lastStage = nStage;
	}, function (a) {
		for (var i = 0; i < a.child.length; i++) {
			var dep = new Dependency(a, a.dependency[i].srcGridArrayId.id, i, nStage)
			for (var j = 0; j < a.child[i].lineageArray.length; j++)
				lineages[a.child[i].lineageArray[j]].push(dep);
			a.lineageArray = a.lineageArray.concat(a.child[i].lineageArray);
		}
		if (a.child.length == 0) {
			lineages.push([]);
			var idx = lineages.length - 1;
			a.lineageArray.push(idx);
			lineages[idx].push(new Dependency(a, null, 0, nStage));
		}
		if (a.persistent)
			a.inMemory = true;
		if (a.transformType == 'wide')
			nStage--;
	});

	// ----------------------------------------------------------------------------------------- //
	// Step 2: for each lineage find last in-memory array and remove previous transforms from lineage
	// ----------------------------------------------------------------------------------------- //
	for (var i = 0; i < lineages.length; i++) {
		var spliceIdx = 0;
		for (var j = 0; j < lineages[i].length; j++) {
			lineages[i][j].stage = lastStage - lineages[i][j].stage;
			if (lineages[i][j].inMemory)
				spliceIdx = j + 1;
		}
		lineages[i].splice(0, spliceIdx);
	}

	// ----------------------------------------------------------------------------------------- //
	// Step 3: Build task stages
	// ----------------------------------------------------------------------------------------- //
	var stage = new Array(lastStage + 1);
	for (var i = 0; i < stage.length; i++)
		stage[i] = [];
	for (var i = 0; i < lineages.length; i++) {
		for (var j = 0; j < lineages[i].length; j++) {
			if (stage[lineages[i][j].stage].length == 0) {
				stage[lineages[i][j].stage].push([]);
				idx = stage[lineages[i][j].stage].length - 1;				
			}
			stage[lineages[i][j].stage][idx].push(lineages[i][j]);
		}
	}
	if (stage[stage.length - 1].length == 0)
		stage[stage.length - 1].push([]);

	// ----------------------------------------------------------------------------------------- //
	// Step 4: Write source code constructor
	// ----------------------------------------------------------------------------------------- //
	var sourceCode = '';
	for (var i = 0; i < worker.length; i++)
		sourceCode += 'worker[' + i + '] = "' + worker[i] + '";\n';
	for (var i = 0; i < stage.length; i++) {
		sourceCode += 'stage[' + i + '] = ';
		sourceCode += (i == (stage.length - 1)) ?
			buildStage(stage[i], i, stage.length, action) :
			buildStage(stage[i], i, stage.length);
		sourceCode += ';\n';
	}

	// console.log(sourceCode)

	return {
		lineages: lineages,
		action: action,
		lastStr: taskTemplate.toString().replace('"WORKERS_AND_STAGES"', sourceCode)
	}
}

function buildStage(lineages, stageIdx, nStages, action) {
	var taskStr, alreadyPersistent = {};

	taskStr = 'function(){\n';
	taskStr += action ? action.init : 'var res = {}\n '
	taskStr += 'var transforms = {};\n';

	for (var l = 0; l < lineages.length; l++) {
		// -------------------------------------------------------------------------- //
		// action dataset already in RAM (run action immediately)
		// -------------------------------------------------------------------------- //
		if (lineages[l].length == 0) {
			if ((stageIdx > 0 ) && (nStages > 1)) {
				taskStr += 'var input = STAGE_RAM;\n';
			} else {
				taskStr += 'var input = RAM[action.srcId];\n';
			}
			taskStr += 'for (var p in input)\n';
			if (action.fun == 'reduce') {
				taskStr += 'for (var i = 0; i < input[p].length; i++)\n';
				taskStr += '\tres = reducer(res, input[p][i]);\n';			// Add user additional arguments to reducer here
			} else if (action.fun == 'count') {
				taskStr += 'res += input[p].length;';
			} else if (action.fun == 'collect') {
				taskStr += 'res = res.concat(input[p]);';
			}
			continue;
		}
		// -------------------------------------------------------------------------- //
		// Dataset has a lineages[0] to be computed (ie. not in RAM)
		// -------------------------------------------------------------------------- //
		if (lineages[l][0].srcId) {
			// First dataset in lineages[0] depends on another dataset already in RAM or in STAGE_RAM
			// taskStr += 'var input = RAM[lineages[' + l + '][0].srcId];\n';
			// if (lineages[l][0].stage == 1)
			// 	taskStr += 'var input = STAGE_RAM;\n';
			// else
			// 	taskStr += 'var input = RAM[lineages[' + l + '][0].srcId];\n';

			if ((stageIdx > 0 ) && (nStages > 1)) {
				taskStr += 'var input = STAGE_RAM;\n';
			} else {
				taskStr += 'var input = RAM[lineages[' + l + '][0].srcId];\n';
			}

		} else {
			// First dataset depends on external ressources (file, function, etc.)
			if (lineages[l][0].transform == 'loadTestData')
				taskStr += 'var input = ml.loadTestData(lineages[' + l + '][0].args[0], lineages[' + l + '][0].args[1], lineages[' + l + '][0].args[2]);\n';
			else if (lineages[l][0].transform == 'parallelize') {
				taskStr += 'var input = lineages[' + l + '][0].args[0];\n';
			} else
				throw 'ugrid-array.js error: ' + lineages[l][0].transform + ' not yet supported'
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
		// for (var i = 0; i < lineages[l].length; i++) {
		// 	taskStr += lineages[l][i].pipeline(l, i);
		// 	if ((lineages[l][i].persistent) && (lineages[l][i].transformType == 'narrow'))
		// 		taskStr += '\t\tRAM[lineages[' + l + '][' + i + '].destId][p].push(tmp);\n';			
		// }

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
		if (action)
			taskStr += action.run;
		taskStr += '\t}\n}\n';
	}

	// taskStr = 'function(){\n' + taskStr + 'return res;\n}';
	taskStr += 'return res;\n}';

	return taskStr;
}

var taskTemplate = function(grid, ml, STAGE_RAM, RAM, lineages, action, callback) {
	var stageIdx = 0;
	var stage = [];
	var shuffle = [];
	var worker = [];

	"WORKERS_AND_STAGES"

	function shuffle(uuid, args, callback) {
		grid.send_cb('request', {uuid: uuid, payload: {cmd: "shuffle", args: args}}, callback);
	}

	function runStage(idx) {
		var res = stage[idx]();
		if (++stageIdx < stage.length) {
			// Map partitions to workers
			var map = worker.map(function(n) {return []});
			for (var i in res) {
				var j = i % worker.length; // Ok if partition name is a Number for now, use hashcoding later
				map[j].push([res[i]]);
			}
			// Shuffle data
			for (var i = 0; i < map.length; i++) {
				if (worker[i] == grid.uuid) {
					STAGE_RAM = map[i];
				} else
					shuffle(grid, worker[i], map[i], function(err, res) {if (err) throw err;});
			}
			// Run next stage if needed
			if (worker.length == 1)
				runStage(stageIdx);
		} else
			callback(res);
	}

	this.run = function() {runStage(0);}
	this.processShuffle = function(data) {
		console.log(data);
		throw 'ugrid-array.js error: processShuffle not yet implemented'
	}
}

module.exports.buildTask = buildTask;