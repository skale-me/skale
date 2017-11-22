const t = require('tape');
const aws = require('aws-sdk');
const sc = require('skale').context();

const skip = (process.env.AWS_ACCESS_KEY_ID || process.env.APPVEYOR) ? false : true;
const s3 = skip ? null : new aws.S3({httpOptions: {timeout: 3600000}, signatureVersion: 'v4'});
const savedir = 's3://skale-test-eu-west-1/test/save';

t.test('save s3', {skip: skip}, function (t) {
  t.plan(3);

  deleteS3Dir('skale-test-eu-west-1', 'test/save/', function (err) {
    t.ok(!err, 'delete S3 previous saved test data');
    sc.range(10)
      .save(savedir, function (err) {
        t.ok(!err, 'save returns no error');
        sc.textFile(savedir + '/')
          .map(a => JSON.parse(a))
          .collect(function (err, res) {
            t.deepEqual(res, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9], 'saved content is correct');
          });
      });
  });
});

t.test('save s3 gzip', {skip: skip}, function (t) {
  t.plan(3);

  deleteS3Dir('skale-test-eu-west-1', 'test/save/', function (err) {
    t.ok(!err, 'delete S3 previous saved test data');
    sc.range(10)
      .save(savedir, {gzip: true}, function (err) {
        t.ok(!err, 'save returns no error');
        sc.textFile(savedir + '/')
          .map(a => JSON.parse(a))
          .collect(function (err, res) {
            t.deepEqual(res, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9], 'saved content is correct');
            sc.end();
          });
      });
  });
});

if (skip) sc.end();

function deleteS3Dir(bucket, prefix, done) {
  function getList(list, token, done) {
    s3.listObjectsV2({
      Bucket: bucket,
      Prefix: prefix,
      ContinuationToken: token
    }, function (err, data) {
      if (err) throw new Error('s3.listObjectsV2 failed');
      list = list.concat(data.Contents);
      if (data.IsTruncated)
        return getList(list, data.NextContinuationToken, done);
      done(err, list);
    });
  }

  getList([], null, function (err, res) {
    if (!res || !res.length) return done();
    s3.deleteObjects({
      Bucket: bucket,
      Delete: {
        Objects: res.map(o => ({Key: o.Key}))
      }
    }, done);
  });
}
