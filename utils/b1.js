#!/usr/local/bin/node --harmony

var fs = require('fs');

// TODO : forEach, take, top, takeOrdered, takeSample, partitionByKey, sortByKey
// LATER: fold aggregate combineByKey subtractByKey foldByKey countByKey

process.stderr.write('\r');
for (var i = 2; i < process.argv.length; i++)
	process.stderr.write(process.argv[i] + ' ');
process.stderr.write('\t\t\t\t');

var tmpdir = '/tmp/' + process.env.USER;
var sources = ['parallelize', 'textFile', 'stream'];
var transfos = ['rightOuterJoin', 'leftOuterJoin', 'intersection', 'subtract', 'crossProduct', 'coGroup', 'join', 'union', 'map', 'filter', 'flatMap', 'mapValues',
	'sample', 'groupByKey', 'reduceByKey', 'distinct', 'flatMapValues',
	'keys', 'values'];
var actions = ['count', 'collect', 'reduce', 'lookup', 'countByValue',
'countStream', 'collectStream', 'reduceStream', 'lookupStream', 'countByValueStream'];

var dist = '', desc = '#', loc = '', src_cnt = -1;
var last_source_id, last_source_type;
var streamOut = false;
var hasKeysOrValues = false;

for (var i = 2; i < process.argv.length; i++) {
	desc += ' ' + process.argv[i];
	var isSource = sources.indexOf(process.argv[i]) != -1;
	var isTransfo = transfos.indexOf(process.argv[i]) != -1;
	var isAction = actions.indexOf(process.argv[i]) != -1;
	var isPersist = (process.argv[i] == 'persist');

	if (isSource) {
		src_cnt++;
		last_source_id = src_cnt;
		last_source_type = process.argv[i];
		if (process.argv[i] == 'parallelize') {
			dist += 'dsource[' + src_cnt + '] = uc.parallelize(v[' + src_cnt + ']);\n\t';
		} else if (process.argv[i] == 'textFile') {
			dist += 'dsource[' + src_cnt + '] = uc.textFile("' + tmpdir + '/v' + src_cnt + '").map(function(e) {return e.split(" ").map(parseFloat)});\n\t';
		} else if (process.argv[i] == 'stream') {
			dist += 'dsource[' + src_cnt + '] = uc.lineStream(s[' + src_cnt + '], {N: 5}).map(textParser);\n\t';
		} else throw new Error('Unknown source :' + process.argv[i]);
		loc += 'lsource[' + src_cnt + '] = v_ref[' + src_cnt + '];\n\t';
	} else if (isTransfo) {
		dist += 'dsource[' + src_cnt + '] = dsource[' + src_cnt + ']';
		loc += 'lsource[' + src_cnt + '] = ';
		switch (process.argv[i]) {
		case 'rightOuterJoin':
			dist += '.rightOuterJoin(dsource[' + (src_cnt - 1) + ']);\n\t';
			loc += 'ut.join(lsource[' + src_cnt + '], lsource[' + (src_cnt - 1) + '], "right");\n\t';
			break;
		case 'leftOuterJoin':
			dist += '.leftOuterJoin(dsource[' + (src_cnt - 1) + ']);\n\t';
			loc += 'ut.join(lsource[' + src_cnt + '], lsource[' + (src_cnt - 1) + '], "left");\n\t';
			break;
		case 'intersection':
			dist += '.intersection(dsource[' + (src_cnt - 1) + ']);\n\t';
			loc += 'ut.intersection(lsource[' + src_cnt + '], lsource[' + (src_cnt - 1) + ']);\n\t';
			break;
		case 'subtract':
			dist += '.subtract(dsource[' + (src_cnt - 1) + ']);\n\t';
			loc += 'ut.subtract(lsource[' + src_cnt + '], lsource[' + (src_cnt - 1) + ']);\n\t';
			break;
		case 'crossProduct':
			dist += '.crossProduct(dsource[' + (src_cnt - 1) + ']);\n\t';
			loc += 'ut.crossProduct(lsource[' + src_cnt + '], lsource[' + (src_cnt - 1) + ']);\n\t';
			break;
		case 'coGroup':
			dist += '.coGroup(dsource[' + (src_cnt - 1) + ']);\n\t';
			loc += 'ut.coGroup(lsource[' + src_cnt + '], lsource[' + (src_cnt - 1) + ']);\n\t';
			break;
		case 'join':
			dist += '.join(dsource[' + (src_cnt - 1) + ']);\n\t';
			loc += 'ut.join(lsource[' + src_cnt + '], lsource[' + (src_cnt - 1) + ']);\n\t';
			break;
		case 'union':
			dist += '.union(dsource[' + (src_cnt - 1) + ']);\n\t';
			loc += 'ut.union(lsource[' + src_cnt + '], lsource[' + (src_cnt - 1) + ']);\n\t';
			break;
		case 'map':
			dist += '.map(mapper);\n\t';
			loc += 'lsource[' + src_cnt + '].map(mapper);\n\t';
			break;
		case 'filter':
			dist += '.filter(filter);\n\t';
			loc += 'lsource[' + src_cnt + '].filter(filter);\n\t';
			break;
		case 'flatMap':
			dist += '.flatMap(flatMapper);\n\t';
			loc += 'lsource[' + src_cnt + '].map(flatMapper).reduce(function(a, b) {return a.concat(b)}, []);\n\t';
			break;
		case 'mapValues':
			dist += '.mapValues(valueMapper);\n\t';
			loc += 'lsource[' + src_cnt + '].map(function(e) {return [e[0], valueMapper(e[1])]});\n\t';
			break;
		case 'sample':
			dist += '.sample(withReplacement, frac);\n\t';
			loc += 'ut.sample(lsource[' + src_cnt + '], uc.worker.length, withReplacement, frac, seed);\n\t';
			break;
		case 'groupByKey':
			dist += '.groupByKey();\n\t';
			loc += 'ut.groupByKey(lsource[' + src_cnt + ']);\n\t';
			break;
		case 'reduceByKey':
			dist += '.reduceByKey(reducerByKey, 0);\n\t';
			loc += 'ut.reduceByKey(lsource[' + src_cnt + '], reducerByKey, 0);\n\t';
			break;
		case 'distinct':
			dist += '.distinct();\n\t';
			loc += 'ut.distinct(lsource[' + src_cnt + ']);\n\t';
			break;
		case 'flatMapValues':
			dist += '.flatMapValues(valueFlatMapper);\n\t';
			loc += 'ut.flatMapValues(lsource[' + src_cnt + '], valueFlatMapper);\n\t';
			break;
		case 'keys':
			dist += '.keys();\n\t';
			loc += 'lsource[' + src_cnt + '].map(function(e){return e[0]});\n\t';
			hasKeysOrValues = true;
			break;
		case 'values':
			dist += '.values();\n\t';
			loc += 'lsource[' + src_cnt + '].map(function(e){return e[1]});\n\t';
			hasKeysOrValues = true;
			break;
		default: throw new Error('Unknown transformation :' + process.argv[i]);
		}
	} else if (isAction) {
		switch (process.argv[i]) {
		case 'count':
			dist += 'var dist = yield dsource[' + src_cnt + '].count();\n\t';
			loc += 'var loc = lsource[' + src_cnt + '].length;\n\t';
			break;
		case 'countStream':
			streamOut = true;
			dist += 'var dist = [];\n\t' +
				'var out = dsource[' + src_cnt + '].count({stream: true});\n\t';
			loc += 'var loc = [lsource[' + src_cnt + '].length];\n\t';
			break;
		case 'collect':
			dist += 'var dist = yield dsource[' + src_cnt + '].collect();\n\t';
			loc += 'var loc = lsource[' + src_cnt + '];\n\t';
			break;
		case 'collectStream':
			streamOut = true;
			dist += 'var dist = [];\n\t' +
				'var out = dsource[' + src_cnt + '].collect({stream: true});\n\t';
			loc += 'var loc = [lsource[' + src_cnt + ']];\n\t';
			break;
		case 'reduce':
			if (hasKeysOrValues) {
				dist += 'var dist = yield dsource[' + src_cnt + '].reduce(reducerByKey, 0);\n\t';
				loc += 'var loc = lsource[' + src_cnt + '].reduce(reducerByKey, 0);\n\t';
			} else {
				dist += 'var dist = yield dsource[' + src_cnt + '].reduce(reducer, [0, 0]);\n\t';
				loc += 'var loc = lsource[' + src_cnt + '].reduce(reducer, [0, 0]);\n\t';
			}
			break;
		case 'reduceStream':
			streamOut = true;
			if (hasKeysOrValues) {
				dist += 'var dist = [];\n\t' +
					'var out = dsource[' + src_cnt + '].reduce(reducerByKey, 0, {stream: true});\n\t';
				loc += 'var loc = [lsource[' + src_cnt + '].reduce(reducerByKey, 0)];\n\t';
			} else {
				dist += 'var dist = [];\n\t' +
					'var out = dsource[' + src_cnt + '].reduce(reducer, [0, 0], {stream: true});\n\t';
				loc += 'var loc = [lsource[' + src_cnt + '].reduce(reducer, [0, 0])];\n\t';
			}
			break;
		case 'lookup':
			dist += 'var dist = yield dsource[' + src_cnt + '].lookup(key);\n\t';
			loc += 'var loc = lsource[' + src_cnt + '].filter(function (e) {return (e[0] == key)});\n\t';
			break;
		case 'lookupStream':
			streamOut = true;
			dist += 'var dist = [];\n\t' +
				'var out = dsource[' + src_cnt + '].lookup(key, {stream: true});\n\t';
			loc += 'var loc = [lsource[' + src_cnt + '].filter(function (e) {return (e[0] == key)})];\n\t';
			break;
		case 'countByValue':
			dist += 'var dist = yield dsource[' + src_cnt + '].countByValue();\n\t';
			loc += 'var loc = ut.countByValue(lsource[' + src_cnt + ']);\n\t';
			break;
		case 'countByValueStream':
			streamOut = true;
			dist += 'var dist = [];\n\t' +
				'var out = dsource[' + src_cnt + '].countByValue({stream: true});\n\t';
			loc += 'var loc = [ut.countByValue(lsource[' + src_cnt + '])];\n\t';
			break;
		default: throw new Error('Unknown action :' + process.argv[i]);
		}
	} else if (isPersist) {
		dist += 'dsource[' + src_cnt + '] = dsource[' + src_cnt + '].persist();\n\t';
		dist += 'yield dsource[' + src_cnt + '].count();\n\t';
		if (last_source_type == 'parallelize') {
			dist += 'v[' + last_source_id + '].push([v_ref[' + last_source_id + '][0][0], v_ref[' + last_source_id + '][0][1]]);\n\t';
		} else if (last_source_type == 'stream') {
		} else if (last_source_type == 'textFile') {
			dist += 'fs.appendFileSync("' + tmpdir + '/v' + last_source_id + '", "\\n1 2\\n");\n\t';
		} else throw new Error('Unknown source: ' + process.argv[i - 1]);
	} else
		throw 'Unknown command: ' + process.argv[i];
}

if (streamOut) {
	dist += '\n\tout.on("data", function (d) {dist.push(d);});' +
		'\n\tout.on("end", function() {compareResults(loc, dist);});';
} else {
	dist += '\n\tcompareResults(loc, dist);';
}

var template = fs.readFileSync('utils/template.js', {encoding: 'utf8'});

var code = template.replace('"DESCRIPTION"', '"' + desc + '"');
code = code.replace('"SOURCE_CODE"', loc + '\n\t' + dist);

console.log(code);
