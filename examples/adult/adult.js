#!/usr/bin/env node

// var ml = require('skale-ml');
var ml = require('../../lib/ml.js');
var sc = require('skale-engine').context();

var metadata = {
	workclass: ['?', 'Private', 'Self-emp-not-inc', 'Self-emp-inc', 'Federal-gov', 'Local-gov', 'State-gov', 'Without-pay', 'Never-worked'],
	education: ['?', 'Bachelors', 'Some-college', '11th', 'HS-grad', 'Prof-school', 'Assoc-acdm', 'Assoc-voc', '9th', '7th-8th', '12th', 'Masters', '1st-4th', '10th', 'Doctorate', '5th-6th', 'Preschool'],
	maritalstatus: ['?', 'Married-civ-spouse', 'Divorced', 'Never-married', 'Separated', 'Widowed', 'Married-spouse-absent', 'Married-AF-spouse'],
	occupation: ['?', 'Tech-support', 'Craft-repair', 'Other-service', 'Sales', 'Exec-managerial', 'Prof-specialty', 'Handlers-cleaners', 'Machine-op-inspct', 'Adm-clerical', 'Farming-fishing', 'Transport-moving', 'Priv-house-serv', 'Protective-serv', 'Armed-Forces'],
	relationship: ['?', 'Wife', 'Own-child', 'Husband', 'Not-in-family', 'Other-relative', 'Unmarried'],
	race: ['?', 'White', 'Asian-Pac-Islander', 'Amer-Indian-Eskimo', 'Other', 'Black'],
	sex: ['?', 'Female', 'Male'],
	nativecountry: ['?', 'United-States', 'Cambodia', 'England', 'Puerto-Rico', 'Canada', 'Germany', 'Outlying-US(Guam-USVI-etc)', 'India', 'Japan', 'Greece', 'South', 'China', 'Cuba', 'Iran', 'Honduras', 'Philippines', 'Italy', 'Poland', 'Jamaica', 'Vietnam', 'Mexico', 'Portugal', 'Ireland', 'France', 'Dominican-Republic', 'Laos', 'Ecuador', 'Taiwan', 'Haiti', 'Columbia', 'Hungary', 'Guatemala', 'Nicaragua', 'Scotland', 'Thailand', 'Yugoslavia', 'El-Salvador', 'Trinadad&Tobago', 'Peru', 'Hong', 'Holand-Netherlands']
}

function featurize(data, metadata) {
	var label = ((data[14] == '>50K') || (data[14] == '>50K.')) ? 1 : -1, features = [];

	features[0] = Number(data[0]);								// age
	features[1] = metadata.workclass.indexOf(data[1]);			// workclass
	if (features[1] == 0) return [];
	features[2] = Number(data[2]);								// fnlwgt
	features[3] = metadata.education.indexOf(data[3]);			// education
	if (features[3] == 0) return [];	
	features[4] = Number(data[4]);								// education-num
	features[5] = metadata.maritalstatus.indexOf(data[5]);		// marital-status
	if (features[5] == 0) return [];
	features[6] = metadata.occupation.indexOf(data[6]);			// occupation	
	if (features[6] == 0) return [];
	features[7] = metadata.relationship.indexOf(data[7]);		// relationship	
	if (features[7] == 0) return [];
	features[8] = metadata.race.indexOf(data[8]);				// race
	if (features[8] == 0) return [];
	features[9] = metadata.sex.indexOf(data[9]);				// sex	
	if (features[9] == 0) return [];
	features[10] = Number(data[10]);							// capital-gain
	features[11] = Number(data[11]);							// capital-loss
	features[12] = Number(data[12]);							// hours-per-week
	features[13] = metadata.nativecountry.indexOf(data[13]);	// native-country
	if (features[13] == 0) return [];
	return [label, features];
}

function normalize(point) {
	var label = point[0], features = point[1], norm = 0, normalized_features = [];
	for (var i = 0; i < features.length; i++)
		norm += Math.pow(features[i], 2);
	for (var i = 0; i < features.length; i++)
		normalized_features[i] = features[i] / Math.sqrt(norm);
	return [label, normalized_features];
}

// Train model
var training_set = sc.textFile('adult.data')
	.map(line => line.split(',').map(str => str.trim()))
	.map(featurize, metadata)
	.filter(point => (point.length > 0))
	.map(normalize)	
	.persist();

// training_set.collect().on('data', console.log)

var model = new ml.LogisticRegression(training_set);
var nIterations = 100;

model.train(nIterations, function() {
	var accumulator = {pos: 0, neg: 0, error: 0, n: 0, weights: model.weights};

	function reducer(acc, svm) {
		var tmp = 0;
		for (var i = 0; i < acc.weights.length; i++)
			tmp += acc.weights[i] * svm[1][i];
		var tmp2 = 1 / (1 + Math.exp(-tmp));
		var dec = tmp2 > 0.5 ? 1 : -1;
		if (dec == 0) acc.neg++; else acc.pos++;
		if (dec != svm[0]) acc.error++;
		acc.n++;
		return acc;
	}

	function combiner(acc1, acc2) {
		acc1.neg += acc2.neg;
		acc1.pos += acc2.pos;
		acc1.error += acc2.error;
		acc1.n += acc2.n;
		return acc1;
	}

	// Validate model manually
	sc.textFile('adult.data')
	// sc.textFile('adult.test')
		.map(line => line.split(',').map(str => str.trim()))
		.map(featurize, metadata)
		.filter(point => (point.length > 0))
		.map(normalize)
		.aggregate(reducer, combiner, accumulator)
		.on('data', function(result) {
			console.log(result)
			console.log('Test accuracy = ' + (100 - result.error / result.n * 100))
		})
		.on('end', sc.end)
});