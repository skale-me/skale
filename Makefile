all:

.PHONY: all browserify clean lint test

OBJ = lib/ugrid-client-browser.js lib/ugrid-context-browser.js
SRCS = $(filter-out $(OBJ), $(wildcard bin/*.js lib/*.js examples/*.js test/unitTest/*.js utils/*.js mocha-test/*.js))

clean:
	rm -f test-old/automatic/*.js

lint:
	jshint $(filter-out $(OBJS), $(SRCS))

test:
	./node_modules/.bin/mocha
