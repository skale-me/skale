function sizeOf(object) {
    var objectList = [];
    var stack = [object];
    var bytes = 0;
	var i, keys, value;

    //while (stack.length) {
    //    value = stack.pop();
	for (var j = 0; j < stack.length; j++) {
		value = stack[j]

		switch (typeof value) {
        case 'boolean':
            bytes += 4;
			break;
        case 'string':
            bytes += value.length * 2;
			break;
		case 'function':
			bytes += value.toString().length * 2;
			break;
        case 'number':
            bytes += 8;
			break;
		case 'object':
        	//if (objectList.indexOf(value) !== -1) break;
            //objectList.push(value);

			if (Array.isArray(value)) {
				bytes += 8 * value.length;
				for (i in value)
					//stack.push(value[i]);
					bytes += sizeOf(value[i])
            } else {
				for (i in value) {
					bytes += 2 * i.length;
					//stack.push(value[i]);
					bytes += sizeOf(value[i])
				}
			}
			break;
        }
    }
    return bytes;
}

module.exports = sizeOf;
