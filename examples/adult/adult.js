#!/usr/bin/env node

/*
	Adult dataset processing as per http://scg.sdsu.edu/dataset-adult_r/
*/

// var ml = require('skale-ml');
var sc = require('skale-engine').context();
var LogisticRegression = require('../../lib/ml.js').LogisticRegression;
var StandardScaler = require('../../lib/ml.js').StandardScaler;

var metadata = {
	workclass: ['Private', 'Self-emp-not-inc', 'Self-emp-inc', 'Federal-gov', 'Local-gov', 'State-gov', 'Without-pay', 'Never-worked'],
	education: ['Bachelors', 'Some-college', '11th', 'HS-grad', 'Prof-school', 'Assoc-acdm', 'Assoc-voc', '9th', '7th-8th', '12th', 'Masters', '1st-4th', '10th', 'Doctorate', '5th-6th', 'Preschool'],
	maritalstatus: ['Married-civ-spouse', 'Divorced', 'Never-married', 'Separated', 'Widowed', 'Married-spouse-absent', 'Married-AF-spouse'],
	occupation: ['Tech-support', 'Craft-repair', 'Other-service', 'Sales', 'Exec-managerial', 'Prof-specialty', 'Handlers-cleaners', 'Machine-op-inspct', 'Adm-clerical', 'Farming-fishing', 'Transport-moving', 'Priv-house-serv', 'Protective-serv', 'Armed-Forces'],
	relationship: ['Wife', 'Own-child', 'Husband', 'Not-in-family', 'Other-relative', 'Unmarried'],
	race: ['White', 'Asian-Pac-Islander', 'Amer-Indian-Eskimo', 'Other', 'Black'],
	sex: ['Female', 'Male'],
	nativecountry: ['United-States', 'Cambodia', 'England', 'Puerto-Rico', 'Canada', 'Germany', 'Outlying-US(Guam-USVI-etc)', 'India', 'Japan', 'Greece', 'South', 'China', 'Cuba', 'Iran', 'Honduras', 'Philippines', 'Italy', 'Poland', 'Jamaica', 'Vietnam', 'Mexico', 'Portugal', 'Ireland', 'France', 'Dominican-Republic', 'Laos', 'Ecuador', 'Taiwan', 'Haiti', 'Columbia', 'Hungary', 'Guatemala', 'Nicaragua', 'Scotland', 'Thailand', 'Yugoslavia', 'El-Salvador', 'Trinadad&Tobago', 'Peru', 'Hong', 'Holand-Netherlands']
}

function featurize(data, metadata) {
	var label = ((data[14] == '>50K') || (data[14] == '>50K.')) ? 1 : -1;
	var features = [
		Number(data[0]),								// age
		metadata.workclass.indexOf(data[1]),			// workclass
		Number(data[2]),								// fnlwgt
		// metadata.education.indexOf(data[3]),			// education (redundant with next feature)
		Number(data[4]),								// education-num
		metadata.maritalstatus.indexOf(data[5]),		// marital-status
		metadata.occupation.indexOf(data[6]),			// occupation	
		metadata.relationship.indexOf(data[7]),		// relationship	
		metadata.race.indexOf(data[8]),				// race
		metadata.sex.indexOf(data[9]),				// sex	
		Number(data[10]),							// capital-gain
		Number(data[11]),							// capital-loss
		Number(data[12]),							// hours-per-week
		metadata.nativecountry.indexOf(data[13])	// native-country	
	];
	return [label, features];
}

var training_set = sc.textFile('adult.data')
	.map(line => line.split(',').map(str => str.trim()))		// split csv lines
	.filter(data => data.indexOf('?') == -1)					// remove incomplete data
	.map(featurize, metadata)									// transform string data to number
	.persist();

var features = training_set.map(point => point[1]);
var scaler = new StandardScaler();

scaler.fit(features, function done() {
	// console.log('MEAN ready ');
	// console.log(scaler.mean)
	// console.log('STD ready ');
	// console.log(scaler.std)

	function standardize(point, args) {
		var label = point[0];
		var features = point[1];		
		var features_std = [];
		for (var i in features)
			features_std[i] = (features[i] - args.mean[i]) / args.std[i];
		return [label, features_std];
	}

	// Standardize training set features and make them persistent for gradient computation
	var training_set_std = training_set.map(standardize, {mean: scaler.mean, std: scaler.std}).persist();
	var model = new LogisticRegression(training_set_std);
	var nIterations = 50;

	model.train(nIterations, function() {
		var accumulator = {tp: 0, tn: 0, fp: 0, fn: 0, n: 0, weights: model.weights};

		function reducer(acc, svm) {
			var margin = 0, label = svm[0], features = svm[1];
			for (var i = 0; i < acc.weights.length; i++)
				margin += acc.weights[i] * features[i];
			var pred_label = 1 / (1 + Math.exp(-margin)) > 0.5 ? 1 : -1;

			if (pred_label == -1) {
				if (label == -1) acc.tn++;
				else acc.fn++;
			} else {
				if (label == -1) acc.fp++;
				else acc.tp++;
			}

			if (pred_label == -1) acc.neg++; else acc.pos++;
			if (pred_label != label) acc.error++;
			acc.n++;
			return acc;
		}

		function combiner(acc1, acc2) {
			acc1.tp += acc2.tp;
			acc1.tn += acc2.tn;
			acc1.fp += acc2.fp;						
			acc1.fn += acc2.fn;
			acc1.n += acc2.n;
			return acc1;
		}

		// Autovalidation
		training_set_std
			.aggregate(reducer, combiner, accumulator)
			.on('data', function(result) {
				console.log(result)
				// var precision = result.tp / (result.tp + result.fp);
				// var recall = result.tp / (result.tp + result.fn);
				// var f1_score1 = 2 * precision * recall / (precision + recall)
				var f1_score = 2 * result.tp / (2 * result.tp + result.fn + result.fp);
				var accuracy = (result.tp + result.tn) / result.n;
				console.log('F1 score = ' + f1_score);
				console.log('Accuracy = ' + accuracy);
			})
			.on('end', sc.end)
	});

	// training_set_std.collect().on('data', console.log)
	// sc.end();
})
