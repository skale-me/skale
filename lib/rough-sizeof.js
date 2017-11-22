// Copyright 2016 Luca-SAS, licensed under the Apache License 2.0

'use strict';

module.exports = function sizeof(obj) {
  let size = 0;

  function sizeOf(obj) {
    if (obj === undefined || obj === null) return size;
    switch (typeof obj) {
    case 'number':
      size += 8;
      break;
    case 'string':
      size += obj.length * 2;
      break;
    case 'boolean':
      size += 4;
      break;
    case 'object':
      if (obj instanceof Array) {
        size += 8 * obj.length;
        for (let i = 0; i < obj.length; i++) sizeOf(obj[i]);
      } else {
        for (let i in obj) {
          size += i.length * 2;
          sizeOf(obj[i]);
        }
      }
      break;
    }
    return size;
  }
  return sizeOf(obj);
};
