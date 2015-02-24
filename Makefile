all: browserify

.PHONY: all lint browserify

OBJ = lib/ugrid-client-browser.js lib/ugrid-context-browserify.js
SRCS = $(filter-out $(OBJ), $(wildcard bin/*.js lib/*.js examples/*.js test/unitTest/*.js))

browserify:
	browserify lib/ugrid-client.js --standalone Client > lib/ugrid-client-browser.js
	browserify lib/ugrid-context.js --standalone UgridContext > lib/ugrid-context-browser.js

lint:
	jshint $(filter-out $(OBJS),$(SRCS))
