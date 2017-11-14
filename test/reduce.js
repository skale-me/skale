var t = require('tape');
var sc = require('skale').context();

t.test('reduce callback', function (t) {
  t.plan(1);
  sc.parallelize([1, 2, 3, 4], 2)
    .reduce((a, b) => a + b, 0, function(err, res) {
      t.equal(res, 10);
    });
});

t.test('reduce promise', function (t) {
  t.plan(1);
  sc.parallelize([1, 2, 3, 4], 2)
    .reduce((a, b) => a + b, 0)
    .then(function(res) {
      t.equal(res, 10);
      sc.end();
    });
});
