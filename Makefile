build:
	browserify --node -i browserify -i node-parquet -i bufferutil -i utf-8-validate -s skale -o skale-node-bundle.js index.js
	browserify -i browserify -i node-parquet -i bufferutil -i utf-8-validate -s skale -o skale-web-bundle.js index.js
