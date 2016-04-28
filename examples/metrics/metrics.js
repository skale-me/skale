#!/usr/bin/env node

// Adult dataset processing as per http://scg.sdsu.edu/dataset-adult_r/
var co = require('co');
var sc = require('skale-engine').context();
var ml = require('skale-ml');

var LogisticRegressionWithSGD = ml.LogisticRegressionWithSGD;
var StandardScaler = ml.StandardScaler;
var BinaryClassificationMetrics = ml.BinaryClassificationMetrics;

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
		Number(data[0]),								// 1 age
		metadata.workclass.indexOf(data[1]),			// 2 workclass
		Number(data[2]),								// 3 fnlwgt
		// metadata.education.indexOf(data[3]),			// education (redundant with education-num)
		Number(data[4]),								// 4 education-num
		metadata.maritalstatus.indexOf(data[5]),		// 5 marital-status
		metadata.occupation.indexOf(data[6]),			// 6 occupation	
		metadata.relationship.indexOf(data[7]),			// 7 relationship	
		metadata.race.indexOf(data[8]),					// 8 race
		metadata.sex.indexOf(data[9]),					// 9 sex	
		Number(data[10]),								// 10 capital-gain
		Number(data[11]),								// 11 capital-loss
		Number(data[12]),								// 12 hours-per-week
		metadata.nativecountry.indexOf(data[13])		// 13 native-country
	];
	return [label, features];
}

var training_set = sc.textFile('adult.data')
	.map(line => line.split(',').map(str => str.trim()))		// split csv lines
	.filter(data => data.indexOf('?') == -1)					// remove incomplete data
	.map(featurize, metadata)									// transform string data to number
	.persist();

var test_set = sc.textFile('adult.test')
	.map(line => line.split(',').map(str => str.trim()))		// split csv lines
	.filter(data => data.indexOf('?') == -1)					// remove incomplete data
	.map(featurize, metadata);									// transform string data to number


// Co is workaround until async/await is available
co(function* () {
	// Standardize features to zero mean and unit variance
	var features = training_set.map(point => point[1]);	
	var scaler = new StandardScaler();

	yield scaler.fit(features);			// async

	function standardize(point, args) {
		var label = point[0], features = point[1], features_std = [];

		for (var i in features)
			features_std[i] = (features[i] - args.mean[i]) / args.std[i];

		return [label, features_std];
	}

	// Transform features into standardized features
	var training_set_std = training_set.map(standardize, {mean: scaler.mean, std: scaler.std}).persist();
	var test_set_std = test_set.map(standardize, {mean: scaler.mean, std: scaler.std});

	// Train logistic regression with SGD on standardized training set
	var model = new LogisticRegressionWithSGD(training_set_std, {regParam: 0.01, stepSize: 1});
	var nIterations = 100;

	yield model.train(nIterations);		// async

	// Evaluate classifier performance on standardized test set
	var predictionAndLabels = test_set_std.map(function(point, args) {
		var margin = 0, label = point[0], features = point[1];

		for (var i = 0; i < features.length; i++)
			margin += args.model.weights[i] * features[i];
		var prediction = 1 / (1 + Math.exp(-margin));

		return [prediction, label];
	}, {model: model});

	var metrics = new BinaryClassificationMetrics(predictionAndLabels);

	console.log('\n# Precision by threshold');
	var precision = yield metrics.precisionByThreshold();
	precision.forEach(p => console.log("Threshold: %d, Precision: %d", p[0].toFixed(2), p[1].toFixed(2)));

	console.log('\n# Recall by threshold');
	var recall = yield metrics.recallByThreshold();
	recall.forEach(p => console.log("Threshold: %d, Recall: %d", p[0].toFixed(2), p[1].toFixed(2)));

	console.log('\n# Precision-recall curve: TODO !!');
	// var prc = yield metrics.pr();

	console.log('\n# F1-score by threshold');
	var beta = 1;
	var f1Score = yield metrics.fMeasureByThreshold(1);
	f1Score.forEach(p => console.log("Threshold: %d, F-score: %d", p[0].toFixed(2), p[1].toFixed(2), beta));

	console.log('\n# Area under precision-recall curve: TODO !!')

	console.log('\n# Receiver Operating characteristic (ROC)')
	var roc = yield metrics.roc();
	console.log('\nThreshold\tSpecificity(FPR)\tSensitivity(TPR)')
	for (var i in roc)
		console.log(roc[i][0].toFixed(2).replace('.', ',') + '\t' + roc[i][1][0].toFixed(2).replace('.', ',') + '\t' + roc[i][1][1].toFixed(2).replace('.', ','));

	console.log('\n# Area under ROC curve: TODO !!')

	console.log('\n# Accuracy by threshold')
	var accuracy = yield metrics.accuracyByThreshold();
	console.log('\nThreshold\tAccuracy')
	for (var i in accuracy)
		console.log(accuracy[i][0].toFixed(2).replace('.', ',') + '\t' + accuracy[i][1].toFixed(2).replace('.', ','));

	sc.end();
});
