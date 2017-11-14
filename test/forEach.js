const t = require('tape');
const sc = require('skale').context();

t.test('forEach', function (t) {
  t.plan(1);

  sc.range(5).forEach((b) => console.log('# b', b), () => {
    t.pass('nothing on master');
    sc.end();
  });
});
