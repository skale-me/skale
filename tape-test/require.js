var t = require('tape');
var sc = require('skale-engine').context();
t.onFinish(sc.end);

var mydep = function (a) {
    return a + 2;
};

function mapper(element, option, wc) {
  console.log('global.fs:', global.fs);
  return wc.lib.mydep(element);
}

t.test('require', function (t) {
  t.plan(1);
  sc.modules.mydep = mydep;
  sc.range(4)
    .map(mapper)
    .collect(function (err, res) {
      console.log('res:', res);
      t.pass('ok');
    });
});
