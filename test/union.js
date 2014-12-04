#!/usr/local/bin/node --harmony

var ml = require('../ugrid-ml.js');

var W = 2;	// nb workers
var M = 2;  // taille du vecteur a
var N = 2;  // taille du vecteur b
var a = [];
var b = [];

// init vecteur a
for (var i = 0; i < W; i++)
	a[i] = ml.randn(M);

// init vecteur b
for (var i = 0; i < W; i++)
	b[i]= ml.randn(N);

//~ console.log('a :');
//~ console.log(a);
//~ 
//~ console.log('\nb :');
//~ console.log(b);
// concatener tous les elements de a dans A en utisent un for
//~ var A=[];
//~ for (i=0;i<M; i++){
	//~ A= A.concat(a[i])   
//~ }	
//~ 
//~ console.log(A)

// concatener tous les elements de a (et b) dans A (et B) en utilisant reduce
// x valeur cumule y valeur current   [] valeur initial       
var A = a.reduce(function(x,y){return x.concat(y)},[]);
var B = b.reduce(function(x,y){return x.concat(y)},[]);
//~ console.log('\nconcat(a) :');
//~ console.log(A);
//~ console.log('\nconcat(b) :');
//~ console.log(B);

c = A.concat(B);  // union de A et B
//~ console.log('\na.union(b) :');
//~ console.log(c);

var json = {
	name: 'union',
	inputs: {
		a: A,
		b: B
	},
	output: c
}

console.log(JSON.stringify(json, null, '\t'))


