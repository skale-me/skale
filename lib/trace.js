function getStack() {
	var orig = Error.prepareStackTrace;
	Error.prepareStackTrace = function (_, stack) {return stack;};
	var err = new Error;
	Error.captureStackTrace(err, arguments.callee);
	var stack = err.stack;
	Error.prepareStackTrace = orig;
	return stack;
}

module.exports = function trace() {
	var args = Array.prototype.slice.call(arguments);
	var stack = getStack();
	var file = stack[1].getFileName().replace(/.*\//, '');
	var line = stack[1].getLineNumber();
	var func = stack[1].getFunctionName() || 'anonymous';

	if (args.length === 1) {
		args.splice(0, 0, '[%s:%s %s] %j', file, line, func);
	} else {
		args[0] = '[%s:%s %s] ' + args[0];
		args.splice(1, 0, file, line, func);
	}
	console.log.apply(this, args);
};
