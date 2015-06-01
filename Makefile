all: browserify

.PHONY: all browserify clean lint gentest

TMP = /tmp/$(USER)
OBJ = lib/ugrid-client-browser.js lib/ugrid-context-browser.js
SRCS = $(filter-out $(OBJ), $(wildcard bin/*.js lib/*.js examples/*.js test/unitTest/*.js utils/*.js))

LAST_TEST = test/automatic/t2819.js

browserify:
	browserify lib/ugrid-client.js --standalone Client > lib/ugrid-client-browser.js
	browserify lib/ugrid-context.js --standalone UgridContext > lib/ugrid-context-browser.js

clean:
	rm -f test/automatic/*.js

lint:
	jshint $(filter-out $(OBJS), $(SRCS))

gentest: $(LAST_TEST)

$(LAST_TEST): utils/gen_test_script.js utils/b1.js utils/template.js $(TMP)/kv.data $(TMP)/kv2.data
	utils/gen_test_script.js test/automatic | sh

$(TMP)/kv.data: test/automatic/kv.data
	mkdir -p $(TMP)
	cp test/automatic/kv.data $(TMP)/

$(TMP)/kv2.data: test/automatic/kv2.data
	mkdir -p $(TMP)
	cp test/automatic/kv2.data $(TMP)/
