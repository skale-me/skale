/*
TODO:
	- for each stage create shuffle request command to be applied
	- integrate STAGE_RAM as stage input and handle persistency
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

function buildTask(worker, array, action) {
	var node = {}, nStage = 0, lastStage = 0, num = 0, transform = '';
	var loop = 'for (var p in input) {\n' +
		'\tfor (var i = 0; i < input[p].length; i++) {\n' +
		'\t\tvar tmp = input[p][i];\n';

	// ---------------------------------------------------------------------- //
	// Treewalk 1: Set stage index in reverse order and node index
	// ---------------------------------------------------------------------- //
	treewalk(array, function (a) {
		if (a.transformType == 'wide')
			nStage++;
		if (lastStage < nStage)
			lastStage = nStage;
	}, function (a) {
		a.stageIdx = nStage;
		a.num = num++;
		if (a.transformType == 'wide')
			nStage--;
	});
	// ---------------------------------------------------------------------- //
	// Treewalk 2: Restore stage index, gen lineages sourcecode and set persistency
	// ---------------------------------------------------------------------- //
	var structure = new Array(lastStage + 1);
	for (var i = 0; i < structure.length; i++)
		structure[i] = {input: [], pipeline: []};

	treewalk(array, null, function (a) {
		node[a.num] = {args: a.args};				// Transfo's args structure
		a.stageIdx = lastStage - a.stageIdx;		// Reverse stage index
		// If we leave an inMemory array, a new stage begin
		if (a.inMemory) {
			if ((a.stageIdx > 0 ) && (stage.length > 1))
				var tmp = 'var input = STAGE_RAM;\n'
			else
				var tmp = 'var input = RAM[' + a.id + '];\n';
			structure[a.stageIdx] = {input: [tmp], pipeline: ['']};
		} else if (a.child.length == 0) {
			// If we leave a leaf, add a new lineage to current stage
			switch (a.transform) {
			case 'loadTestData':
				var tmp = 'var input = ml.loadTestData(node[' + a.num + 
					'].args[0], ' + 'node[' + a.num + '].args[1], node[' + 
					a.num + '].args[2]);\n';
				break;
			case 'parallelize':
				var tmp = 'var input = node[' + a.num + '].args[0];\n';
				break;
			default:
				throw 'unknown transform in second treewalk'
			}
			if (a.persistent)
				tmp += 'RAM[' + a.id + '] = input;\n';
			structure[a.stageIdx].input.push(tmp);
			structure[a.stageIdx].pipeline.push('');
		} else {
			// if we're not leaving a leaf and a is not in Memory
			// pipeline current transformation to all lineages of current stage
			switch (a.transform) {
			case 'map':
				var argStr = '';
				for (var arg = 0; arg < a.args[1].length; arg++)
					argStr += ', node[' + a.num + '].args[1][' + arg + ']';
				var tmp = '\t\ttmp = transform[' + a.num + '](tmp' + argStr + ');\n';
				transform += 'transform[' + a.num + '] = ' + a.args[0].toString() + '\n';
				break;
			case 'union':
				break;
			case 'reduceByKey':
				var key = 'node[' + a.num + '].args[0]';
				var tmp = 'if (tmp[' + key + '] == undefined)\n\tthrow "key unknown by worker"\n';
				tmp += 'if (res[tmp[' + key + ']] == undefined)\n' +
					'\tres[tmp[' + key + ']] = JSON.parse(JSON.stringify(node[' + a.num + '].args[2]));\n';
				tmp += 'res[tmp[' + key + ']] = transform[' + a.num + '](res[tmp[' + key + ']], tmp);\n';
				transform += 'transform[' + a.num + '] = ' + a.args[1].toString() + '\n';
				break;
			default:
				throw a.transform + ' error: pipeline call not implemented'
			}
			if ((a.persistent) && (a.transformType == 'narrow'))
				tmp += '\t\tRAM[' + a.id + '][p].push(tmp);\n';
			for (var i = 0; i < structure[a.stageIdx].pipeline.length; i++)
				structure[a.stageIdx].pipeline[i] += tmp;
		}
		// Set persistency for next build
		a.inMemory = a.persistent;
	});

	// ---------------------------------------------------------------------- //
	// If the array on which action is triggered is already in Memory
	// or the stage index is higher than 0 and number of stages > 1
	// ---------------------------------------------------------------------- //	
	// if (lineages[l].length == 0) {
	// 	if ((stageIdx > 0 ) && (nStages > 1)) {
	// 		taskStr += 'var input = STAGE_RAM;\n';
	// 	} else {
	// 		taskStr += 'var input = RAM[' + array.id + '];\n';
	// 	}
	// 	taskStr += 'for (var p in input)\n';
	// 	if (action.fun == 'reduce') {
	// 		taskStr += 'for (var i = 0; i < input[p].length; i++)\n';
	// 		taskStr += '\tres = reducer(res, input[p][i]);\n';			// Add user additional arguments to reducer here
	// 	} else if (action.fun == 'count') {
	// 		taskStr += 'res += input[p].length;';
	// 	} else if (action.fun == 'collect') {
	// 		taskStr += 'res = res.concat(input[p]);';
	// 	}
	// 	continue;
	// }

	// ---------------------------------------------------------------------- //
	// Step 3: Write source code of Task() constructor
	// ---------------------------------------------------------------------- //
	var sourceCode = '';
	for (var i = 0; i < worker.length; i++)
		sourceCode += 'worker[' + i + '] = "' + worker[i] + '";\n';
	sourceCode += transform;
	for (var i = 0; i < structure.length; i++) {
		sourceCode += 'stage[' + i + '] = function() {\n';
		sourceCode += (i == lastStage) ? action.init : 'var res = {};\n';
		for (var j = 0; j < structure[i].pipeline.length; j++) {
			sourceCode += structure[i].input[i];
			sourceCode += loop;
			sourceCode += structure[i].pipeline[j];
			if (i == lastStage)
				sourceCode += action.run;
			sourceCode += '\t}\n}\n';
		}
		sourceCode += 'return res;\n}\n';
	}	

	var finalSrc = taskTemplate.toString().replace('"WORKERS_AND_STAGES"', sourceCode);
	return {node: node, action: action, lastStr: finalSrc}
}

var taskTemplate = function(grid, ml, STAGE_RAM, RAM, node, action, callback) {
	var stageIdx = 0, nShuffle = 0;
	var worker = [], stage = [], shuffle = [], transform = {};

	"WORKERS_AND_STAGES"

	function shuffleRPC(uuid, args, callback) {
		grid.send_cb('request', {uuid: uuid, payload: {cmd: "shuffle", args: args}}, callback);
	}

	function runStage(idx) {
		var lastStage = (stageIdx == (stage.length - 1));
		var res = stage[idx]();
		if (lastStage) {
			STAGE_RAM = [];
			callback(res);			
		} else {
			// Map partitions to workers
			var map = worker.map(function(n) {return []});
			for (var i in res) {
				var j = i % worker.length; // Ok if partition name is a Number for now, use hashcoding later
				map[j][i] = [res[i]];
			}
			// Shuffle data
			for (var i = 0; i < map.length; i++) {
				if (worker[i] == grid.uuid) {
					nShuffle++;
					STAGE_RAM = map[i];
				} else {
					// remove undefined key
					var obj = {};
					for (var p = 0; p < map[i].length; p++) {
						if (map[i][p] == undefined) continue;
						obj[p] = map[i][p];
					}
					shuffleRPC(worker[i], obj, function(err, res) {if (err) throw err;});
				}
			}
			// Run next stage if needed
			if (worker.length == 1)
				runStage(++stageIdx);
		}
	}

	this.run = function() {runStage(0);}
	this.processShuffle = function(data) {
		// Reduce intermediates results (to be generated programmatically)
		for (p in STAGE_RAM) {
			if (!STAGE_RAM[p]) continue;
			if (!data[p]) continue;
			for (var i = 0; i < STAGE_RAM[p][0].acc.length; i++)
				STAGE_RAM[p][0].acc[i] += data[p][0].acc[i];
				STAGE_RAM[p][0].sum += data[p][0].sum;
		}

		if (++nShuffle == worker.length) {
			nShuffle = 0;
			runStage(++stageIdx);
		} else
			console.log('Shuffle stucked');
	}
}

module.exports.buildTask = buildTask;