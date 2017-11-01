var t = require('tape');
var sc = require('skale-engine').context();
t.onFinish(sc.end);

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
