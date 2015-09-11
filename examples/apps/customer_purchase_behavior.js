#!/usr/local/bin/node --harmony
'use strict';

var co = require('co');
var ugrid = require('../..');

var nProducts = 4;
var nCategories = 4;
var nBaskets = 4;

// generate random product categories
var d = [];
for (var i = 0; i < nProducts; i++)
	d[i] = {category: Math.ceil(Math.random() * nCategories)};

// generate random baskets of 2 products
var b = [];
for (var i = 0; i < nBaskets; i++)
	b[i] = [Math.ceil(Math.random() * nProducts) - 1, Math.ceil(Math.random() * nProducts) - 1];


console.log('# products')
console.log(d)

console.log('\n# baskets')
console.log(b)

// compute Third(d[i]), the number of baskets that contain a product of the same category that d_i
var norm_thr_di = [];
for (var i = 0; i < d.length; i++) {
	var cat = d[i].category;
	var n = 0;
	for (var j = 0; j < b.length; j++) {
		if ((d[b[j][0]].category == cat) || (d[b[j][1]].category == cat))
			n++;
	}
	norm_thr_di[i] = n;
}

console.log('\n# norm_thr_di')
console.log(norm_thr_di)

// compute (Third(d[i]) inter Third(d[j])), the number of baskets that contain a product of the same category that d_i and d_j
var norm_thr_di_dj = [];
for (var i = 0; i < d.length; i++) {
	var cat_di = d[i].category;
	norm_thr_di_dj[i] = [];
	for (var j = 0; j < d.length; j++) {
		var cat_dj = d[j].category;
		var n = 0;		
		for (var k = 0; k < b.length; k++) {
			var ok = true;
			if ((d[b[k][0]].category != cat_di) && (d[b[k][1]].category != cat_di)) ok = false;
			if ((d[b[k][0]].category != cat_dj) && (d[b[k][1]].category != cat_dj)) ok = false;
			if (ok) n++;
		}
		norm_thr_di_dj[i][j] = n;
	}
}

console.log('\n# norm_thr_di_dj')
console.log(norm_thr_di_dj)

var p_dj_di = [];
for (var i = 0; i < d.length; i++) {
	p_dj_di[i] = [];
	for (var j = 0; j < d.length; j++)
		p_dj_di[i][j] = norm_thr_di_dj[i][j] / norm_thr_di[j]
}

console.log('\n# p(d_j|d_i)')
console.log(p_dj_di)

// for (var i = 0; i < d.length; i++) {
// 	var product_idx = [i];
// 	for (var j = 0; j < d.length; j++) {
// 		if (i == j) continue;
// 		// si une des categories de d_j match une des categories de d_i on ajoute i à la liste des produits de d_i
// 		for (var c = 0; c < d[j].category.length; c++)
// 			if (d[i].category.indexOf(d[j].category[c]) != -1) break;
// 		if (c < d[j].category.length) product_idx.push(j);
// 	}
// 	d[i].friendly_products = product_idx;
// }

// // compute Third(d[i]) inter Third(d[j]) for any i, j
// var cross_product = [];
// for (var i = 0; i < d.length; i++) {
// 	cross_product[i] = [];
// 	// intersection des catgeories du produit i avec celle du produit j
// 	for (var j = 0; j < d.length; j++) {
// 		var data = [];
// 		if (i == j) data = d[i].friendly_products;
// 		else {
// 			for (var p = 0; p < d[i].friendly_products.length; p++) {
// 				if (d[j].friendly_products.indexOf(d[i].friendly_products[p]) != -1) {
// 					data.push(d[i].friendly_products[p]);
// 					continue;
// 				}
// 			}
// 		}
// 		cross_product[i].push(data);
// 	}

// 	d[i].p_dj_knowing_di = [];
// 	for (var j = 0; j < d.length; j++) {
// 		d[i].p_dj_knowing_di[j] = cross_product[i][j].length / d[i].friendly_products.length;
// 	}
// }

// console.log('# Products')
// console.log(d)

// console.log('\n# crossproduct')
// console.log(cross_product);


// Compute 


// var card_d = [];
// for (var j = 0; j < d.length; j++)
// 	card_d[j] = d[j].category.length;

// console.log(card_d)

// var card_d_cross_d = [];
// for (var i = 0; i < card_d.length; i++) {
// 	for (var j = 0; j < d.length; j++) {
// 		card_d_cross_d[j] = d[j].category.length;)
// 	}
// }
	