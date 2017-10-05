#!/usr/bin/env node

'use strict';

// Wrap code into ES7 async function, to limit callback imbrications
(async function main() {

  // Adult dataset processing as per http://scg.sdsu.edu/dataset-adult_r/
  // In this example we:
  // - train a logistic regression on the adult training set.
  // - evaluate the model on the adult test set
  // - generate ROC curves as png images

  var sc = require('skale-engine').context();
  var ml = require('skale-engine/ml');
  var plot = require('plotter').plot;     // Todo: should be replaced by D3

  // Todo: features should be automatically extracted from dataset + type schema
  var metadata = {
    workclass: [
      'Private', 'Self-emp-not-inc', 'Self-emp-inc', 'Federal-gov', 'Local-gov',
      'State-gov', 'Without-pay', 'Never-worked'
    ],
    education: [
      'Bachelors', 'Some-college', '11th', 'HS-grad', 'Prof-school', 'Assoc-acdm',
      'Assoc-voc', '9th', '7th-8th', '12th', 'Masters', '1st-4th', '10th', 'Doctorate',
      '5th-6th', 'Preschool'
    ],
    maritalstatus: [
      'Married-civ-spouse', 'Divorced', 'Never-married', 'Separated', 'Widowed',
      'Married-spouse-absent', 'Married-AF-spouse'
    ],
    occupation: [
      'Tech-support', 'Craft-repair', 'Other-service', 'Sales', 'Exec-managerial',
      'Prof-specialty', 'Handlers-cleaners', 'Machine-op-inspct', 'Adm-clerical',
      'Farming-fishing', 'Transport-moving', 'Priv-house-serv', 'Protective-serv',
      'Armed-Forces'
    ],
    relationship: [
      'Wife', 'Own-child', 'Husband', 'Not-in-family', 'Other-relative', 'Unmarried'
    ],
    race: [
      'White', 'Asian-Pac-Islander', 'Amer-Indian-Eskimo', 'Other', 'Black'
    ],
    sex: [
      'Female', 'Male'
    ],
    nativecountry: [
      'United-States', 'Cambodia', 'England', 'Puerto-Rico', 'Canada', 'Germany',
      'Outlying-US(Guam-USVI-etc)', 'India', 'Japan', 'Greece', 'South', 'China',
      'Cuba', 'Iran', 'Honduras', 'Philippines', 'Italy', 'Poland', 'Jamaica',
      'Vietnam', 'Mexico', 'Portugal', 'Ireland', 'France', 'Dominican-Republic',
      'Laos', 'Ecuador', 'Taiwan', 'Haiti', 'Columbia', 'Hungary', 'Guatemala',
      'Nicaragua', 'Scotland', 'Thailand', 'Yugoslavia', 'El-Salvador',
      'Trinadad&Tobago', 'Peru', 'Hong', 'Holand-Netherlands'
    ]
  };

  function featurize(data, metadata) {
    var label = (data[14] === '>50K' || data[14] === '>50K.') ? 1 : -1;
    var features = [
      Number(data[0]),                          // 1 age
      metadata.workclass.indexOf(data[1]),      // 2 workclass
      Number(data[2]),                          // 3 fnlwgt
      // metadata.education.indexOf(data[3]),   // education (redundant with education-num)
      Number(data[4]),                          // 4 education-num
      metadata.maritalstatus.indexOf(data[5]),  // 5 marital-status
      metadata.occupation.indexOf(data[6]),     // 6 occupation
      metadata.relationship.indexOf(data[7]),   // 7 relationship
      metadata.race.indexOf(data[8]),           // 8 race
      metadata.sex.indexOf(data[9]),            // 9 sex
      Number(data[10]),                         // 10 capital-gain
      Number(data[11]),                         // 11 capital-loss
      Number(data[12]),                         // 12 hours-per-week
      metadata.nativecountry.indexOf(data[13])  // 13 native-country
    ];
    return [label, features];
  }

  var trainingSet = sc.textFile(__dirname + '/adult.data')
    .map(line => line.split(',').map(str => str.trim()))              // split csv lines
    .filter(data => data.length === 15 && data.indexOf('?') === -1)   // remove incomplete data
    .map(featurize, metadata)                                         // transform string data to number
    .persist();

  var testSet = sc.textFile(__dirname + '/adult.test')
    .map(line => line.split(',').map(str => str.trim()))              // split csv lines
    .filter(data => data.length === 15 && data.indexOf('?') === -1)   // remove incomplete data
    .map(featurize, metadata);                                        // transform string data to number

  // Standardize features to zero mean and unit variance
  var scaler = new ml.StandardScaler();

  await scaler.fit(trainingSet.map(point => point[1]));

  // Use scaler to standardize training and test datasets
  var trainingSetStd = trainingSet
    .map((p, scaler) => [p[0], scaler.transform(p[1])], scaler);

  var testSetStd = testSet
    .map((p, scaler) => [p[0], scaler.transform(p[1])], scaler);

  // Train logistic regression with SGD on standardized training set
  var nIterations = 10;
  var parameters = {loss: 'log', penalty: 'l2', regParam: 0.001, stepSize: 1, proba: true};
  var model = new ml.SGDClassifier(parameters);

  await model.fit(trainingSetStd, nIterations);

  var predictionAndLabels = testSetStd.map((p, model) => [model.predict(p[1]), p[0]], model);
  var metrics = await ml.binaryClassificationMetrics(predictionAndLabels, {steps: 10});

  console.log('model weights:', model.weights);
  console.log('intercept:', model.intercept);
  console.log('ROC curve: roc.png');
  console.log('ROC AUC:', metrics.auroc);
  console.log('Best threshold (F1 max):', metrics.threshold);
  sc.end();

  // Plot ROC curve
  const xy = {'0.00': 0};
  for (let i = 0; i < metrics.rates.length; i++)
    xy[metrics.rates[i].fpr.toFixed(2)] = metrics.rates[i].recall;
  xy['1.00'] = 1;
  const data = {};
  data['regParam: ' + parameters.regParam + ', stepSize: ' + parameters.stepSize] = xy;
  data['Random'] = {'0.00': 0, 1: 1};
  plot({
    title: 'Logistic Regression ROC Curve',
    data: data,
    filename: 'roc.png',
  });

})(); // main
