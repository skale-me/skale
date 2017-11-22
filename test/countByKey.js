const t = require('tape');
const sc = require('skale').context();

t.test('countByKey callback', function (t) {
  t.plan(1);

  sc.parallelize([[1, 2], [3, 4], [3, 6]])
    .countByKey(function (err, res) {
      t.deepEqual(res.sort(), [[1, 1], [3, 2]]);
    });
});

t.test('countByKey promise', function (t) {
  t.plan(1);

  sc.parallelize([[1, 2], [3, 4], [3, 6]])
    .countByKey()
    .then(function(res) {
      t.deepEqual(res.sort(), [[1, 1], [3, 2]]);
      sc.end();
    });
});
