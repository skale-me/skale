const stream = require('stream');
const zlib =  require('zlib');
const t = require('tape');
const sc = require('skale').context();

t.test('stream', function (t) {
  t.plan(2);

  let res = '';
  const s = sc.range(10).stream();

  t.ok(s instanceof stream.Readable, 'ds.stream() returns a readable stream');

  s.on('data', function (data) {res += data.toString();});
  s.on('end', function () {
    t.equal(res, '0\n1\n2\n3\n4\n5\n6\n7\n8\n\9\n', 'data read is correct');
  });
});

t.test('stream gzip', function (t) {
  t.plan(2);

  let res = '';
  const s = sc.range(10).stream({gzip: true});

  t.ok(s instanceof stream.Readable, 'ds.stream() returns a readable stream');

  const rs = s.pipe(zlib.createGunzip());

  rs.on('data', function (data) {res += data.toString();});
  rs.on('end', function () {
    t.equal(res, '0\n1\n2\n3\n4\n5\n6\n7\n8\n\9\n', 'gunzip data read is correct');
    sc.end();
  });
});
