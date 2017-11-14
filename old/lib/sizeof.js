var _ = require('lodash');

var typeSizes = {
    'boolean': function() { return 4 },
    'number': function() { return 8 },
    'string': function(item) { return 2 * item.length },
    'object': function(item) { return getObjectSize(item) }
};

function sizeOf(value) {return typeSizes[typeof value](value);};

function getObjectSize(object) {
    return _.reduce(object, function(sum, value, key) {
        return sum + sizeOf(value) + sizeOf(key);
    }, 0);
}

module.exports = sizeOf;
