default: standalone distributed

standalone:
	@SKALE_HOST= SKALE_WORKERS=2 tape "./**/*.js"

distributed:
	@../bin/server.js -l 2 >/tmp/skale-server.log 2>&1 & pid=$$!; \
	SKALE_HOST=localhost tape "./**/*.js"; \
	res=$$?; kill $$pid; exit $$res

%:
	SKALE_HOST= SKALE_WORKERS=2 tape "./**/*$@*.js"
