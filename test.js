var hyperquest    = require('hyperquest')
  , jsonist       = require('jsonist')
  , through2      = require('through2')
  , assert        = require('assert')
  , level         = require('level')
  , rimraf        = require('rimraf')
  , moment        = require('moment')
  , NpmDownloadDb = require('./')

var dbdir        = './__test.db.' + process.pid
  , updatedCalls = 0
  , rankedCalls  = 0
  , db
  , timeout


// mock out network calls and make it a quick return
hyperquest.get = function hyperquestGet (url) {
  var s = through2()

  setImmediate(function i () {
    s.end('[{"name":"foobar1"},{"name":"foobar2"},{"name":"foobar3"},{"name":"foobar4"},{"name":"nan"}]')
  })

  return s
}


jsonist._oldget = jsonist.get
jsonist.get = function jsonistGet (url, callback) {
  var re = /https:\/\/api\.npmjs\.org\/downloads\/range\/(20\d\d-\d\d-\d\d):(20\d\d-\d\d-\d\d)\/(?:foobar([1234])|nan)/
    , m = url.match(re)

  assert(m, 'url matched expected regex')
  if (m[3]) { // mock data
    setImmediate(function i () {
      if (m[3] == 2)
        return callback(new Error('registry error: no stats for this package for this range (0008)'))
      if (m[3] == 1 || m[3] == 3) {
        return callback(null, { downloads: [
            { day: moment().utc().add(-20, 'days').format('YYYY-MM-DD'), downloads: 200 }
          , { day: moment().utc().add(-10, 'days').format('YYYY-MM-DD'), downloads: 100 }
        ] })
      }
      callback(null, { downloads: [] })
    })
  } else // nan
      jsonist._oldget(url, callback)
}


db = NpmDownloadDb(level(dbdir))
db.on('updated', onUpdated)
db.update()

function onUpdated () {
  updatedCalls++

  db.rank()
  db.on('ranked', onRanked)
}


function onRanked () {
  rankedCalls++

  clearTimeout(timeout)
  final()
}


timeout = setTimeout(final, 5000)


function final () {
  var nowS = moment().utc().add(-1, 'days').format('YYYY-MM-DD')

  assert.equal(1, updatedCalls, 'correct number of "updated" events')
  assert.equal(1, rankedCalls, 'correct number of "ranked" events')

  db.packageCount('foobar1', moment().utc().add(-10, 'days').toDate(), function afterCount (err, count) {
    assert.ifError(err)
    assert.equal(100, count, 'correct count (' + count + ' == ' + 100 + ')')
  })
  db.packageCount('foobar1', moment().utc().add(-11, 'days').toDate(), function afterCount (err, count) {
    assert.ifError(err)
    assert.equal(0, count, 'correct count (' + count + ' == ' + 0 + ')')
  })
  db.packageCount('foobar1', moment().utc().add(-20, 'days').toDate(), function afterCount (err, count) {
    assert.ifError(err)
    assert.equal(200, count, 'correct count (' + count + ' == ' + 200 + ')')
  })
  db.packageCount('foobar1', moment().utc().add(-1, 'year').toDate(), new Date(), function afterCount (err, count) {
    assert.ifError(err)
    assert.equal(300, count, 'correct count (' + count + ' == ' + 300 + ')')
  })
  db.packageCount('foobar1', moment().utc().add(-1, 'year').toDate(), moment().utc().add(-21, 'days').toDate(), function afterCount (err, count) {
    assert.ifError(err)
    assert.equal(0, count, 'correct count (' + count + ' == ' + 0 + ')')
  })
  db.packageCount('nan', moment().utc().add(-1, 'year').toDate(), new Date(), function afterCount (err, count) {
    assert.ifError(err)
    assert(count > 200000, 'reasonable count for nan in a year (' + count + ')')
  })
  db.packageCount('nan', moment().utc().add(-10, 'days').toDate(), function afterCount (err, count) {
    assert.ifError(err)
    assert(count > 10000, 'reasonable count for nan in a day (' + count + ')')
  })

  assert.equal(4, db.allPackages.length, 'correct number of packages')
  assert(db.allPackages.indexOf('foobar1') > -1, 'has foobar1')
  assert.equal(-1, db.allPackages.indexOf('foobar2'), 'does not have foobar2')
  assert(db.allPackages.indexOf('foobar3') > -1, 'has foobar3')
  assert(db.allPackages.indexOf('foobar4') > -1, 'has foobar4')
  assert(db.allPackages.indexOf('nan') > -1, 'has nan')

  db.packageRank('nan', function afterRank (err, rankData) {
    assert.ifError(err)
    assert.equal('nan', rankData.package, 'correct package name (' + rankData.package + ')')
    assert.equal(1, rankData.rank, 'correct rank (' + rankData.rank + ')')
    assert.equal(nowS, rankData.day, 'correct ranking day')
  })
  db.packageRank('foobar1', function afterRank (err, rankData) {
    assert.ifError(err)
    assert.equal('foobar1', rankData.package, 'correct package name (' + rankData.package + ')')
    assert.equal(2, rankData.rank, 'correct rank (' + rankData.rank + ')')
    assert.equal(nowS, rankData.day, 'correct ranking day')
  })
  db.packageRank('foobar2', function afterRank (err, rankData) {
    assert(err, 'got error from foobar2 fetch')
  })
  db.packageRank('foobar3', function afterRank (err, rankData) {
    assert.ifError(err)
    assert.equal('foobar3', rankData.package, 'correct package name (' + rankData.package + ')')
    assert.equal(2, rankData.rank, 'correct rank (' + rankData.rank + ')')
    assert.equal(nowS, rankData.day, 'correct ranking day')
  })
  // same as foobar3
  db.packageRank('foobar4', function afterRank (err, rankData) {
    assert.ifError(err)
    assert.equal('foobar4', rankData.package, 'correct package name (' + rankData.package + ')')
    assert.equal(4, rankData.rank, 'correct rank (' + rankData.rank + ')')
    assert.equal(nowS, rankData.day, 'correct ranking day')
  })

  setTimeout(function t () {
    db.close(function afterClose () {
      rimraf.sync(dbdir)
    })
  }, 500) // arbitrary but slow enough
}
