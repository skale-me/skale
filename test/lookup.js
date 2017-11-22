const t = require('tape');
const sc = require('skale').context();

t.test('lookup callback', function (t) {
  t.plan(1);

  sc.parallelize([[1, 2], [3, 4], [3, 6]])
    .lookup(3, function(err, res) {
      t.deepEqual(res, [4, 6]);
    });
});

t.test('lookup promise', function (t) {
  t.plan(1);

  sc.parallelize([[1, 2], [3, 4], [3, 6]])
    .lookup(3)
    .then(function(res) {
      t.deepEqual(res, [4, 6]);
      sc.end();
    });
});
