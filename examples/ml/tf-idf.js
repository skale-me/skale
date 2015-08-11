#!/usr/local/bin/node --harmony
'use strict';

var co = require('co');
var ugrid = require('../..');
var ml = require('../../lib/ugrid-ml');

// Example from https://janav.wordpress.com/2013/10/27/tf-idf-and-cosine-similarity/

var corpus = [
	'The game of life is a game of everlasting learning',
	'The unexamined life is not worth living',
	'Never stop learning'
]

co(function *() {
	var uc = yield ugrid.context();

	var input = uc.parallelize(corpus).map(function(doc) {return doc.split(' ')});

	// Compute TF(t, d) vector, the term frequency of each document
	var tf = input.map(function (doc) {
		var TF = {};
		for (var i = 0; i < doc.length; i++) {
			if (TF[doc[i]] == undefined) TF[doc[i]] = 0;
			TF[doc[i]]++;
		}
		// for (var t in TF) TF[t] /= doc.length;	// normalize term freqency (Not Done in SPARK)
		return TF;
	});

	// console.log(yield tf.collect());

	// Compute IDF(t, D) vector 
	var idf = yield tf.aggregate(computeIDF, merge, [{}, 0]);

	function computeIDF(a, b) {
		a[1]++;	// increment document count
		for (var term in b) {
			if (a[0][term] == undefined) a[0][term] = 1;
			else a[0][term]++;
		}
		return a;
	}

	function merge(a, b) {
		a[1] += b[1];
		for (var term in b[0]) {
			if (a[0][term] == undefined) a[0][term] = 0;
			a[0][term] += b[0][term];
		}
		return a;
	}

	// Compute IDF(t, D) on master side
	for (var term in idf[0])
		idf[0][term] = Math.log((1 + idf[1]) / (1 + idf[0][term]));

	// console.log('\nIDF(t, D) vector');
	// console.log(idf[0]);

	// Compute tf-idf(t, d) for each document
	var tfidf = tf.map(function(tf, idf) {
		var tfidf = {};
		for (var term in tf) tfidf[term] = tf[term] * idf[term];
		return tfidf;
	}, [idf[0]]);

	// console.log(yield tfidf.collect());	

	// Finding document relevance on a query
	var query = ['game', 'life'];
	var tfidf_query = {};
	for (var i in query) tfidf_query[query[i]] = 0.5 * idf[0][query[i]];

	// Relevance of the query to each document
	function cosineSimilarity(tfidf_doc, tfidf_query) {		
		var similarity = 0, query_norm = 0, doc_norm = 0;		
		for (var term in tfidf_query) {
			similarity += tfidf_query[term] * (tfidf_doc[term] ? tfidf_doc[term] : 0);
			query_norm += tfidf_query[term] * tfidf_query[term];
			doc_norm += tfidf_doc[term] ? tfidf_doc[term] * tfidf_doc[term] : 0;
		}
		return similarity / (Math.sqrt(query_norm) * Math.sqrt(doc_norm));
	}

	var similarity = yield tfidf.map(cosineSimilarity, [tfidf_query]).collect();

	console.log('query = ' + JSON.stringify(query));
	console.log('Similarity to documents')
	console.log(similarity)

	uc.end();
}).catch(ugrid.onError);
