// Customized 'thenify' to allow a function to return a stream instead of a promise,
// if last argument is an object containing stream: true

//var Promise = require('native-or-bluebird')
var assert = require('assert')
//var trace = require('line-trace')

module.exports = skalify

/**
 * Turn async functions into promises
 *
 * @param {Function} $$__fn__$$
 * @return {Function}
 * @api public
 */

function skalify($$__fn__$$) {
  assert(typeof $$__fn__$$ === 'function')
  return eval(createWrapper($$__fn__$$.name, true))
}

function createCallback(resolve, reject) {
  return function(err, value) {
    if (err) return reject(err)
    var length = arguments.length
    if (length <= 2) return resolve(value)
    var values = new Array(length - 1)
    for (var i = 1; i < length; ++i) values[i - 1] = arguments[i]
    resolve(values)
  }
}

function createWrapper(name, callbackOrStream) {
  callbackOrStream = callbackOrStream ?
    'var lastType = typeof arguments[len - 1]\n'
	  + 'if (lastType === "function" || (lastType === "object" && arguments[len - 1].stream))\n'
	    + 'return $$__fn__$$.apply(self, arguments)\n'
    : ''

  return '(function ' + (name || '') + '() {\n'
    + 'var self = this\n'
    + 'var len = arguments.length\n'
    + callbackOrStream
    + 'var args = new Array(len + 1)\n'
    + 'for (var i = 0; i < len; ++i) args[i] = arguments[i]\n'
    + 'var lastIndex = i\n'
    + 'return new Promise(function (resolve, reject) {\n'
      + 'args[lastIndex] = createCallback(resolve, reject)\n'
      + '$$__fn__$$.apply(self, args)\n'
    + '})\n'
  + '})'
}
