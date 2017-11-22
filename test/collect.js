const t = require('tape');
const sc = require('skale').context();

t.test('collect', function (t) {
  t.plan(1);

  sc.range(6)
    .collect(function (err, res) {
      t.deepEqual(res, [0, 1, 2, 3, 4, 5]);
      sc.end();
    });
});
