const t = require('tape');
const sc = require('skale-engine').context();

t.test('takeSample', function (t) {
  t.plan(1);

  sc.range(100)
    .takeSample(false, 4, function(err, res) {
      console.log(res);
      t.ok(res.length == 4);
      sc.end();
    });
});
