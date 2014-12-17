#!/usr/local/bin/node --harmony

var co = require('co');
var fs = require('fs');
var ugrid = require('../../lib/ugrid-context.js')({host: 'localhost', port: 12346});
var ml = require('../../lib/ugrid-ml.js');

//~ var M = 5;
//~ var N = 4;
//~ var c = a.concat(b);
//~ var d = a.concat(a);

try {
	co(function *() {
		yield ugrid.init();

		var N = 2;
		var D = 2;
			 
		//union
		var d1 = ugrid.loadTestData(N, D);
		var d2 = ugrid.loadTestData(N, D).persist();
		var d3 = d1.union(d2);
		//reflesive union 
		var d4 = d2.union(d2);

		var r1 = yield d1.collect();
		console.error('d1: ');
		console.error(r1);
		
		var r2 = yield d2.collect();
		console.error('\nd2: ');
		console.error(r2);

		var r3 = yield d3.collect();
		console.error('\nd3: ');
		console.error(r3);

		var r4 = yield d4.collect();
		console.error('\nd4: ');
		console.error(r4);
		//use the same data for local union
		
		a = r1[0]['features'];
		b = r1[1]['features'];
		A = a.reduce(function(x,y){return x.concat(y)},[]);
		B = b.reduce(function(x,y){return x.concat(y)},[]);
		
		R30 = r3[0]['features'];
		R31 = r3[1]['features'];
		R3 = R30.reduce(function(x,y){return x.concat(y)},[]);
		R3 = R31.reduce(function(x,y){return x.concat(y)},[]);
		
		R40 = r4[0]['features'];
		R41 = r4[1]['features'];
		R3 = R40.reduce(function(x,y){return x.concat(y)},[]);
		R4 = R41.reduce(function(x,y){return x.concat(y)},[]);
		
		console.error(' ###########');
		console.error('\nA = ');
		console.error(A);
		console.error('\nB = ');
		console.error(B);
		
		C= A.concat(B);
		console.error('\nC = ');
		console.error(C);	
		console.error(' ###########');

		console.error('\nr4 = ');
		console.error(r4);	
		console.error(' ###########');
		//compare r3 and C, union
		test1 = true;
		for (var i = 0; i < C.length; i++) {
			if (r3[i] != C[i]) {
				test1 = false;
				break;
			}				
		}
		//compare r4 and c, reflesive union
		test2 = true;
		for (var i = 0; i < C.length; i++) {
			if (r4[i] != C[i]) {
				test2 = false;
				break;
			}				
		}
		test = false;
		if((test1)&&(test2)) {
			test = true;
		}else test = false;
		
	
		if (test) {
			console.log('test ok')	
			process.exit(0);
		}		
		else {
			console.log('tesk ko')
			process.exit(1);
		}
		ugrid.end();
	})();
} catch (err) {
	process.exit(2);
}

