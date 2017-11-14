var t = require('tape');
var sc = require('skale').context();

t.test('arrow function', function (t) {
  t.plan(1);

  sc.range(6)
    .map((a) => a*a)
    .reduce((a,b) => a+b, 0)
    .then(function (res) {
      t.equal(res, 55);
      sc.end();
    });
});
