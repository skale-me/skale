#!/usr/bin/env node

'use strict';

// Wrap code into ES7 async function, to limit callback imbrications
(async function main() {

  // Adult dataset processing as per http://scg.sdsu.edu/dataset-adult_r/
  // In this example we:
  // - train a logistic regression on the adult training set.
  // - evaluate the model on the adult test set
  // - generate ROC curves as png images

  const sc = require('skale').context();
  const ml = require('skale/ml');
  const plot = require('plotter').plot;     // Todo: should be replaced by D3

  // Todo: features should be automatically extracted from dataset + type schema
  const metadata = {
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
    const label = (data[14] === '>50K' || data[14] === '>50K.') ? 1 : -1;
    const features = [
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

  const trainingSet = sc.textFile(__dirname + '/dataset/adult-0*.csv')
    .filter(a => a[0] !== 'a')                                        // filter out header
    .map(line => line.split(',').map(str => str.trim()))              // split csv lines
    .filter(data => data.length === 15 && data.indexOf('?') === -1)   // remove incomplete data
    .map(featurize, metadata)                                         // transform string data to number
    .persist();

  const testSet = sc.textFile(__dirname + '/dataset/adult-1*.csv')
    .filter(a => a[0] !== 'a')                                        // filter out header
    .map(line => line.split(',').map(str => str.trim()))              // split csv lines
    .filter(data => data.length === 15 && data.indexOf('?') === -1)   // remove incomplete data
    .map(featurize, metadata);                                        // transform string data to number

  // Standardize features to zero mean and unit variance
  const scaler = new ml.StandardScaler();

  await scaler.fit(trainingSet.map(point => point[1]));

  // Use scaler to standardize training and test datasets
  const trainingSetStd = trainingSet
    .map((p, scaler) => [p[0], scaler.transform(p[1])], scaler);

  const testSetStd = testSet
    .map((p, scaler) => [p[0], scaler.transform(p[1])], scaler);

  // Train logistic regression with SGD on standardized training set
  const nIterations = 10;
  const parameters = {loss: 'log', penalty: 'l2', regParam: 0.001, stepSize: 1, proba: true};
  const model = new ml.SGDLinearModel(parameters);

  await model.fit(trainingSetStd, nIterations);

  const predictionAndLabels = testSetStd.map((p, model) => [model.predict(p[1]), p[0]], model);
  const metrics = await ml.classificationMetrics(predictionAndLabels, {steps: 100});

  console.log('model weights:', model.weights);
  console.log('intercept:', model.intercept);
  console.log('PR AUC:', metrics.auPR);
  console.log('ROC AUC:', metrics.auROC);
  console.log('ROC curve: roc.png');
  console.log('Best threshold (F1 max):', metrics.threshold);
  sc.end();

  // Plot ROC curve
  const xy = {};
  for (let i = 0; i < metrics.rates.length; i++)
    xy[metrics.rates[i].fpr || '0.00000000000'] = metrics.rates[i].recall;
  const data = {};
  data['regParam: ' + parameters.regParam + ', stepSize: ' + parameters.stepSize] = xy;
  data['Random'] = {0: 0, 1: 1};
  plot({
    title: 'Logistic Regression ROC Curve',
    data: data,
    filename: 'roc.png',
  });

  // Plot PR curve
  const xy0 = {};
  for (let i = 0; i < metrics.rates.length; i++)
    xy0[metrics.rates[i].recall || '0.00000000000'] = metrics.rates[i].precision;
  const data0 = {};
  data0['regParam: ' + parameters.regParam + ', stepSize: ' + parameters.stepSize] = xy0;
  plot({
    title: 'Logistic Regression PR Curve',
    data: data0,
    filename: 'pr.png',
  });

})(); // main
