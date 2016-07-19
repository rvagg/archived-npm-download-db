# npm-download-db

**A local store containing npm download counts for all packages, able to provide rankings**

[![NPM](https://nodei.co/npm/npm-download-db.png)](https://nodei.co/npm/npm-download-db/)

## API

### `NpmDownloadDb(db[, options])` (constructor)

Returns a new instance of `NpmDownloadDb` operating on the database provided. The database must be a [LevelUp](https://github.com/level/levelup) style object (can be backed by LevelDB or whatever backing store you can find implementing the API).

`options` is optional but if `options.rankPeriod` is provided it will override the default of `30` to set the period, in days, within which to calculate the ranking of packages.

### `db.update([options])`

Run an update operation on the database, using [npm-download-count-collector](https://github.com/level/npm-download-count-collector) to collect downloads for _all_ current packages in the registry. By default, a full year's worth of downloads will be collected at a parallelism of `2`. See npm-download-count-collector for details on the options you can pass through.

Note this can take a while and will result in a large database.

When the update operation is complete, the `db` object will emit an `'updated'` event.

### `db.rank()`

Run a rank operation on the database. Packages are summed for the current period (`rankPeriod` (default `30`) days up to the previous day) and ranked according to totals. Identical download counts will receive the same rank number.

When the rank operation is complete, the `db` object will emit an `'ranked'` event.

### `db.packageCount(package, start[, end], callback)`

Retrieve the download count for a given package over a given period where `start` and `end` are standard `Date` objects. If no `end` date is provided, receive just the downloads for the `start` day. The callback will be provided either with an `Error` (including in the case that the package can't be found) or an integer with the count.

### `db.packageRank(package, callback)`

Retrieve the latest rank for a given package. The callback will be provided either with an `Error` (including in the case that the package can't be found) or an object containing a number of properties related to the rank:

* `package`: the name of the package in question
* `rank`: the ranking of the package over the latest period for which a ranking was calculated for this package
* `day`: the day on which the ranking was taken (recorded as the day prior to the `rank()` operation)
* `count`: the download count for the package during the period for which the ranking was considered

### `db.allPackages`

Contains an `Array` with the names of all of the packages for which download data could be fetched during the last update operation (will be undefined until an update is performed).

## License

**npm-download-db** is Copyright (c) 2016 Rod Vagg ([rvagg](https://github.com/rvagg)) and licensed under the Apache 2.0 licence. All rights not explicitly granted in the Apache 2.0 license are reserved. See the included LICENSE file for more details.
