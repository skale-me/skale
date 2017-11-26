const t = require('tape');
const sc = require('skale').context();
const ml = require('skale/ml');

t.test('SGDLinearModel', function (t) {
  t.plan(3);

  const trainingSet = sc.parallelize([
    [1, [0.5, -0.7]],
    [-1, [-0.5, 0.7]]
  ]);
  const sgd = new ml.SGDLinearModel();

  sgd.fit(trainingSet, 2, function (err) {
    t.ok(!err, 'sgd.fit() returns no error');
    t.deepEqual(sgd.weights, [0.8531998372026804, -1.1944797720837526], 'sgd weights are correct');
    t.ok(sgd.predict([2, -2]) > 0, 'sgd prediction is correct');
    sc.end();
  });
});
