const t = require('tape');
const sc = require('skale').context();
const ml = require('skale/ml');

t.test('standard scaler', function (t) {
  t.plan(4);

  const data = sc.parallelize([[0, 0], [0, 0], [1, 1], [1, 1]]);
  const scaler = new ml.StandardScaler();

  scaler.fit(data, function (err) {
    t.ok(!err, 'scaler.fit() returns no error');
    t.deepEqual(scaler.mean, [0.5, 0.5]);
    t.deepEqual(scaler.std, [0.5, 0.5]);

    const scaled = data.map((p, scaler) => scaler.transform(p), scaler);
    scaled.collect(function (err, res) {
      t.deepEqual(res, [[-1, -1], [-1, -1], [1, 1], [1, 1]], 'scaled data is correct');
      sc.end();
    });
  });
});
