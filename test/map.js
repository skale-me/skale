const t = require('tape');
const sc = require('skale').context();

function by2(a, args) {return a * 2 * args.bias;}
function sum(a, b) {return a + b;}

t.test('map', function (t) {
  t.plan(1);

  sc.parallelize([1, 2, 3, 4])
    .map(by2, {bias: 2})
    .reduce(sum, 0, function(err, res) {
      t.equal(res, 40);
    });
});

t.test('mapValues', function (t) {
  t.plan(1);

  sc.parallelize([['hello', 1], ['world', 2], ['test', 4]])
    .mapValues(a => a * 2)
    .collect(function (err, res) {
      t.deepEqual(res.sort(), [['hello', 2], ['test', 8], ['world', 4]]);
      sc.end();
    });
});
