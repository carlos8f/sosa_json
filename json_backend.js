var fs = require('fs');
var mkdirp = require('mkdirp');
var path = require('path');
var lockfile = require('lockfile');
var crypto = require('crypto');

function PlainObject () {}
PlainObject.prototype = Object.create(null);
function newObj () { return new PlainObject }

module.exports = function (coll_name, backend_options) {
  backend_options || (backend_options = {});
  if (typeof backend_options.hashKeys === 'undefined') backend_options.hashKeys = true;
  if (!backend_options.path) throw new Error('must pass a json file path with backend_options.path');
  if (typeof backend_options.wait === 'undefined') {
    backend_options.wait = 2010;
  }
  if (typeof backend_options.stale === 'undefined') {
    backend_options.stale = 10000;
  }
  if (typeof backend_options.retries === 'undefined') {
    backend_options.retries = 1;
  }
  if (typeof backend_options.retryWait === 'undefined') {
    backend_options.retryWait = 1000;
  }
  var coll_path = coll_name;
  if (backend_options.key_prefix) coll_path = [coll_path, backend_options.key_prefix];
  var collKey = hash(coll_path);

  function hash (id) {
    return backend_options.hashKeys
      ? crypto.createHash('sha1').update(JSON.stringify(id)).digest('hex')
      : Array.isArray(id) ? id.join(':') : id;
  }

  var cache = null;

  return {
    _getColl: function (mem) {
      mem[collKey] || (mem[collKey] = {keys: [], values: newObj()});
      return mem[collKey];
    },
    _readFile: function (cb) {
      if (cache) return cb(null, cache);
      fs.readFile(backend_options.path, {encoding: 'utf8'}, function (err, raw) {
        if (err && err.code === 'ENOENT') {
          return mkdirp(path.dirname(backend_options.path), function (err) {
            cache = newObj()
            cb(null, cache);
          });
        }
        else if (err) return cb(err);
        try {
          var mem = JSON.parse(raw);
        }
        catch (e) {
          return cb(e);
        }
        cache = mem;
        cb(null, mem);
      });
    },
    _writeFile: function (mem, cb) {
      try {
        var raw = JSON.stringify(mem, null, 2);
      }
      catch (e) {
        return cb(e);
      }
      lockfile.lock(backend_options.path + '.lock', backend_options, function (err) {
        if (err) return cb(err);
        fs.writeFile(backend_options.path, raw, function (err1) {
          lockfile.unlock(backend_options.path + '.lock', function (err2) {
            if (err1) return cb(err1);
            if (err2) return cb(err2);
            cb();
          });
        });
      });
    },
    load: function (id, opts, cb) {
      var self = this;
      var idKey = hash(id);
      self._readFile(function (err, mem) {
        if (err) return cb(err);
        var coll = self._getColl(mem);
        cb(null, coll.values[idKey] || null);
      });
    },
    save: function (id, obj, opts, cb) {
      var self = this;
      var idKey = hash(id);
      self._readFile(function (err, mem) {
        if (err) return cb(err);
        var coll = self._getColl(mem);
        coll.values[idKey] = obj;
        if (!~coll.keys.indexOf(id)) coll.keys.push(id);
        self._writeFile(mem, function (err) {
          if (err) return cb(err);
          cb(null, coll.values[idKey]);
        });
      });
    },
    destroy: function (id, opts, cb) {
      var self = this;
      var idKey = hash(id);
      self._readFile(function (err, mem) {
        if (err) return cb(err);
        var coll = self._getColl(mem);
        var obj = coll.values[idKey] || null;
        if (obj) {
          var idx = coll.keys.indexOf(id);
          if (idx !== -1) coll.keys.splice(idx, 1);
          delete coll.values[idKey];
          self._writeFile(mem, function (err) {
            if (err) return cb(err);
            cb(null, obj);
          });
        }
        else cb(null, null);
      });
    },
    select: function (opts, cb) {
      var self = this;
      self._readFile(function (err, mem) {
        if (err) return cb(err);
        var coll = self._getColl(mem);
        var keys = coll.keys.slice();
        if (opts.reverse) keys.reverse();
        var begin = opts.offset || 0;
        var end = opts.limit ? begin + opts.limit : undefined;
        if (begin || end) keys = keys.slice(begin, end);
        var objs = keys.map(function (id) {
          var key = hash(id);
          return coll.values[key] || null;
        });
        cb(null, objs);
      });
    }
  };
};
