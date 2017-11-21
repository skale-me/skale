const t = require('tape');
const azure = require('azure-storage');
const sc = require('skale').context();

const skip = process.env.AZURE_STORAGE_CONNECTION_STRING ? false : true;
const retry = new azure.ExponentialRetryPolicyFilter();
const az = azure.createBlobService().withFilter(retry);
const savedir = 'wasb://skalejs/save';

t.test('save azure', {skip: skip}, function (t) {
  t.plan(3);

  deleteAzureDir('save', '', function (err) {
    t.ok(!err, 'delete previous saved test data');
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

t.test('save azure gzip', {skip: skip}, function (t) {
  t.plan(3);

  deleteAzureDir('save', '', function (err) {
    t.ok(!err, 'delete previous saved test data');
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

function deleteAzureDir(container, prefix, done) {
  function getList(list, token, done) {
    az.listBlobsSegmentedWithPrefix(container, prefix, token, function (err, data) {
      if (err) throw new Error('az.listBlobsSegmented failed');
      list = list.concat(data.entries);
      if (data.continuationToken)
        return getList(list, data.continuationToken, done);
      done(err, list);
    });
  }

  getList([], null, function (err, res) {
    if (!res || !res.length) return done();
    let toDelete = res.length;
    res.forEach(function (element) {
      az.deleteBlob(container, element.name, function (err) {
        if (--toDelete <= 0) done(err);
      });
    });
  });
}
