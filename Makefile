all: browserify

.PHONY: all lint browserify

OBJ = lib/ugrid-client-browser.js
SRCS = $(filter-out $(OBJ), $(wildcard bin/*.js lib/*.js examples/*.js test/unitTest/*.js))

browserify: lib/ugrid-client.js
	browserify lib/ugrid-client.js --standalone UgridClient > lib/ugrid-client-browser.js

lint:
	jshint $(filter-out $(OBJS),$(SRCS))
