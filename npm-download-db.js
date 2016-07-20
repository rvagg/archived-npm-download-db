var downloadCountCollector = require('npm-download-count-collector')
  , moment                 = require('moment')
  , through2               = require('through2')
  , leftPad                = require('left-pad')
  , inherits               = require('inherits')
  , EventEmitter           = require('events')

var defaultRankPeriod = 30


function toKey () {
  return '~~' + Array.prototype.join.call(arguments, '~~') + '~~'
}


function NpmDownloadDb (db, options) {
  var self = this

  if (!(this instanceof NpmDownloadDb))
    return new NpmDownloadDb(db, options)

  EventEmitter.call(this)

  if (db == null)
    throw new TypeError('must provide a "level" style db')

  this._db = db
  this._rankPeriod = options && typeof options.rankPeriod == 'number' ? options.rankPeriod : defaultRankPeriod

  this._db.get(toKey('allPackages'), function afterGet (err, value) {
    if (err) {
      if (!err.notFound)
        self.emit('error', err)
      return
    }
    try {
      self.allPackages = JSON.parse(value)
    } catch (err) {} // ignorable, not too important
  })
}


inherits(NpmDownloadDb, EventEmitter)


NpmDownloadDb.prototype.update = function update (options) {
  var self     = this
    , packages = []

  function packageData (data) {
    var batch = data.downloads.map(function m (d) {
      return { type: 'put', key: toKey('count', data.name, d.day), value: d.count }
    })

    packages.push(data.name)

    // note we don't do do anything special with these async writes, it's very likely
    // that an 'updated' event will be triggered before writes have completed
    self._db.put(toKey('package', data.name), data.name, function afterPut (err) {
      if (err)
        return self.emit('error', err)
    })

    self._db.batch(batch, function afterBatch (err) {
      if (err)
        return self.emit('error', err)
    })
  }

  function finish () {
    self.allPackages = packages
    self._db.put(toKey('allPackages'), JSON.stringify(packages), function afterPut (err) {
      if (err)
        return self.emit('error', err)
    })
    self.emit('updated')
  }

  downloadCountCollector(options)
    .on('packageError', this.emit.bind(this, 'packageError'))
    .on('packageData', packageData)
    .on('error', this.emit.bind(this, 'error'))
    .on('finish', finish)
}


NpmDownloadDb.prototype.rank = function rank () {
  var self      = this
    , start     = moment().utc().add(-(this._rankPeriod) - 1, 'days').toDate()
    , end       = moment().utc().add(-1, 'day').toDate()
    , nowS      = moment(end).format('YYYY-MM-DD')
    , curRank   = 1
    , batchSize = 0
    , prevEntry

  function onPackageChunk (chunk, enc, callback) {
    var pkg = String(chunk)

    self.packageCount(pkg, start, end, function afterCount (err, count) {
      var key, value

      if (err)
        return callback(err)

      key = toKey('periodTotal', self._rankPeriod, nowS, leftPad(count, 12, '0'), pkg)
      value = JSON.stringify({ package: pkg, count: count })

      self._db.put(key, value, callback)
    })
  }

  function onPackageFinish () {
    self._db.valueStream({ gte: toKey('periodTotal', self._rankPeriod, nowS), reverse: true })
      .on('error', this.emit.bind(this, 'error'))
      .pipe(through2.obj(onRankChunk))
      .on('error', this.emit.bind(this, 'error'))
      .on('finish', onRankFinish)
  }

  function onRankChunk (chunk, enc, callback) {
    var entry
      , value

    try {
      entry = JSON.parse(chunk)
    } catch (err) {
      return callback(err)
    }

    if (prevEntry && prevEntry.count !== entry.count) {
      curRank += batchSize
      batchSize = 1
    } else
      batchSize++ // when packages have the same rank we can jump forward by that many for the next lowest

    prevEntry = entry

    value = JSON.stringify({ package: entry.package, rank: curRank, day: nowS, count: entry.count })
    self._db.put(toKey('rank', entry.package, nowS), value, callback)
  }

  function onRankFinish () {
    self.emit('ranked')
  }

  this._db.valueStream({ gte: toKey('package', '!') })
    .on('error', this.emit.bind(this, 'error'))
    .pipe(through2.obj(onPackageChunk))
    .on('error', this.emit.bind(this, 'error'))
    .on('finish', onPackageFinish)
}


NpmDownloadDb.prototype.packageCount = function packageCount (pkg, start, end, callback) {
  var total = 0

  if (typeof end == 'function') { // no end date, just one day
    callback = end
    this._db.get(toKey('count', pkg, moment(start).format('YYYY-MM-DD')), function afterGet (err, data) {
      if (err)
        return callback(err)

      callback(null, parseInt(data, 10))
    })
  } else {
    this._db.valueStream({
        gte: toKey('count', pkg, moment(start).format('YYYY-MM-DD'))
      , lte: toKey('count', pkg, moment(end).format('YYYY-MM-DD'))
    }).on('data', function onData (value) {
      total += parseInt(value, 10)
    }).on('end', function onEnd () {
      callback && callback(null, total)
      callback = null
    }).on('error', function onError (err) {
      callback && callback(err)
      callback = null
    })
  }
}


NpmDownloadDb.prototype.packageCounts = function packageCounts (pkg, start, end, callback) {
  var result = []

  this._db.readStream({
      gte: toKey('count', pkg, moment(start).format('YYYY-MM-DD'))
    , lte: toKey('count', pkg, moment(end).format('YYYY-MM-DD'))
  }).on('data', function onData (data) {
    result.push({ day: data.key.split('~~')[3], count: parseInt(data.value, 10) })
  }).on('end', function onEnd () {
    callback && callback(null, result)
    callback = null
  }).on('error', function onError (err) {
    callback && callback(err)
    callback = null
  })
}


NpmDownloadDb.prototype.packageRank = function packageRank (pkg, callback) {
  this._db.valueStream({ lte: toKey('rank', pkg, '~'), gte: toKey('rank', pkg, '!'), limit: 1, reverse: true })
    .on('error', function onError (err) {
      callback && callback(err)
      callback = null
    })
    .on('data', function onData (data) {
      var value

      if (!callback)
        return

      try {
        value = JSON.parse(data)
      } catch (err) {
        callback(err)
      }

      if (value)
        callback(null, value)

      callback = null
    })
    .on('end', function onEnd () {
      callback && callback(new Error('no rank for package (' + pkg + ') found'))
      callback = null
    })
}


NpmDownloadDb.prototype.close = function close (callback) {
  this._db.close(callback)
}

module.exports = NpmDownloadDb
