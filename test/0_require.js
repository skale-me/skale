const t = require('tape');
const sc = require('skale').context();

t.test('env', function (t) {
  t.plan(1);

  sc.env.MY_VAR = 'hello';
  sc.range(5)
    .map(a => process.env.MY_VAR + a)
    .collect(function (err, res) {
      t.equal(res[0], 'hello0');
    });
});

t.test('require', function (t) {
  t.plan(1);

  sc.require({add3: './dep.js'})
    .range(4)
    .map(a => add3(a))                // eslint-disable-line no-undef
    .collect(function (err, res) {
      t.deepEquals(res, [3, 4, 5, 6]);
      sc.end();
    });
});

