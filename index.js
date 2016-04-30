var sosa = require('sosa')
  , backend = require('./json_backend')

module.exports = function (backend_options) {
  return sosa(backend, backend_options);
};
module.exports.backend = backend;
