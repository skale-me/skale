all: browserify

.PHONY: all lint browserify

OBJS = lib/ugrid-client-browser.js
SRCS = $(wildcard bin/*.js) $(wildcard lib/*.js) $(wildcard examples/*.js) $(wildcard test/unitTest/*)

browserify: lib/ugrid-client.js
	browserify lib/ugrid-client.js --standalone UgridClient > lib/ugrid-client-browser.js

lint:
	jshint $(filter-out $(OBJS),$(SRCS))
