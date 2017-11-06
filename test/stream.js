const stream = require('stream');
const t = require('tape');
const sc = require('skale-engine').context();

t.test('stream', function (t) {
  t.plan(2);

  let res = '';
  const s = sc.range(10).stream();

  t.ok(s instanceof stream.Readable);

  s.on('data', function (data) {res += data.toString();}); 
  s.on('end', function () {
    t.equal(res, '0\n1\n2\n3\n4\n5\n6\n7\n8\n\9\n');
    sc.end();
  });
});
