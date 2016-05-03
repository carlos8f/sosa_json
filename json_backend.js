var fs = require('fs');
var mkdirp = require('mkdirp');
var path = require('path');
var lockfile = require('lockfile');
var crypto = require('crypto');

function PlainObject () {}
PlainObject.prototype = Object.create(null);
function newObj () { return new PlainObject }

var globalCache = newObj();

module.exports = function (coll_name, backend_options) {
  backend_options || (backend_options = {});
  if (typeof backend_options.hashKeys === 'undefined') backend_options.hashKeys = true;
  if (!backend_options.path) throw new Error('must pass a json file path with backend_options.path');
  var coll_path = coll_name;
  if (backend_options.key_prefix) coll_path = [coll_path, backend_options.key_prefix];
  var collKey = hash(coll_path);

  function hash (id) {
    return backend_options.hashKeys
      ? crypto.createHash('sha1').update(JSON.stringify(id)).digest('hex')
      : Array.isArray(id) ? id.join(':') : id;
  }

  return {
    _getColl: function (mem) {
      mem[collKey] || (mem[collKey] = {keys: [], values: newObj()});
      return mem[collKey];
    },
    _readFile: function (cb) {
      if (globalCache[backend_options.path]) return cb(null, globalCache[backend_options.path]);
      try {
        var locked = lockfile.checkSync(backend_options.path + '.lock', backend_options);
        if (locked) {
          var err = new Error('db is locked by another process');
          throw err;
        }
        lockfile.lockSync(backend_options.path + '.lock', backend_options);
        var raw = fs.readFileSync(backend_options.path, {encoding: 'utf8'});
        var mem = JSON.parse(raw);
      }
      catch (err) {
        if (err && err.code === 'ENOENT') {
          mkdirp.sync(path.dirname(backend_options.path));
          globalCache[backend_options.path] = newObj();
          return cb(null, globalCache[backend_options.path]);
        }
        else return cb(err);
      }
      globalCache[backend_options.path] = mem;
      cb(null, globalCache[backend_options.path]);
    },
    _writeFile: function (mem, cb) {
      try {
        var raw = JSON.stringify(mem, null, 2);
        fs.writeFileSync(backend_options.path, raw);
      }
      catch (e) {
        return cb(e);
      }
      setImmediate(cb);
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

module.exports.globalCache = globalCache;