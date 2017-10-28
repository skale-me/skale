var t = require('tape');
var sc = require('skale-engine').context();
t.onFinish(sc.end);

t.test('arrow function', function (t) {
  t.plan(1);

  sc.range(6)
    .map((a) => a*a)
    .reduce((a,b) => a+b, 0)
    .then(function (res) {
      t.equal(res, 55);
    });
});
