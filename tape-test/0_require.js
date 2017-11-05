var t = require('tape');
var sc = require('skale-engine').context();

t.test('require', function (t) {
  t.plan(1);

  sc.require({add3: './dep'})
    .range(4)
    .map(a => add3(a))                // eslint-disable-line no-undef
    .collect(function (err, res) {
      t.deepEquals(res, [3, 4, 5, 6]);
      sc.end();
    });
});
