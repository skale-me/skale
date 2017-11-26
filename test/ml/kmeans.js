const t = require('tape');
const sc = require('skale').context();
const ml = require('skale/ml');

t.test('kmeans', function (t) {
  t.plan(2);

  const dataset = sc.parallelize([
    [1, 2], [1, 4], [1, 0],
    [4, 2], [4, 4], [4, 0]
  ]);
  const kmeans = ml.KMeans(2);
  kmeans.fit(dataset, function (err) {
    t.ok(!err, 'kmeans.fit() returns no error');
    t.ok(kmeans.predict([0, 0]) !== kmeans.predict([4, 4]), 'predictions are correct');
    sc.end();
  });
});
