var t = require('tape');
var sc = require('skale-engine').context();

t.onFinish(sc.end);

function sum(a, b) {return a + b;}

t.test('reduce', function (t) {
  t.plan(2);
  sc.parallelize([1, 2, 3, 4], 2)
    .reduce(sum, 0, function(err, res) {
      t.equal(res, 10);
    });
  sc.parallelize([1, 2, 3, 4], 2)
    .reduce(sum, 0)
    .then(function(res) {
      t.equal(res, 10);
    });
});
