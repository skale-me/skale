ITERATIONS=1
FILE=1MB.dat

benchmark:
	time skale run $(FILE) $(ITERATIONS) > skale.res
	time spark-submit sparkLR.py $(FILE) $(ITERATIONS) > spark.res
	./compare.js