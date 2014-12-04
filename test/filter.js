#!/usr/local/bin/node --harmony

//Example sans workers

var ml = require('../ugrid-ml.js');
var M = 5  // taille du vecteur a
var a =[]


var seed = 1;
function random() {

    var x = Math.sin(seed++);
    return x;
}


function randn(n) {
	var w = new Array(n);
	for (var i = 0; i < n; i++) 
		w[i] = random();
	return w;	
}

//init vecteur a
for (i=0;i<M; i++){
	
	a[i]= random()
}	

console.log('a =')
console.log(a)
console.log('\n')

result=a.filter(function positive(num){ if (num > 0) return true})
console.log(result)


