# Working with Key-Values Distributed Arrays

A Key-values distributed array is structured as follow:

	[[Ki, Vi], … [Kj, Vj]]

It can be seen as an arbitrary-sized Javascript Array where each element is a Javascript Array containing 2 elements, the key and the value, which can be of any serializable type. The developper must ensure that data passed to transformations working on key-value pairs are well-structured.

## Sources


### `parallelize(array)`

### `randomSVMData()`

### `textFile(path)`

## Transformations

All transformations operate on a DA and return a DA, so they can be chained. A transformation is called with the following parameters:

- a user provided helper function, called for each element. The helper function must be
  self-contained, or rely on dependencies accessible through the worker context (see below).
- a user provided additional data, which will be passed to helper function. Those data must be
  serializable (it must be possible to apply `JSON.stringify()` on it)

the helper function, if any, has a form of `helper(element, [[data] [, wc]])`, where:

- *data* is the user additional data as passed to the transformation. It must be serializable.
- *wc* is the worker context, a global object defined in each worker and persistent accross
  transformations. It can be used to extend the worker capabilities through the `wc.require()`
  function as in the following example:

```
function mapper(element, data, wc) {
	if (!wc.maxmind) wc.maxmind = wc.reqire('maxmind');
	return wc.maxmind.getCountry(element);
}
var res = uc.parallelize(vect).map(mapper).collect();
```

### `map(mapper [, obj])`

Applies the provided mapper function to each element of the source DA and returns a new DA.

#### Arguments

 - *mapper*: a function of the form `mapper(element [[,obj] [, wc]])`. It returns an element, and its argument are:
   - *element*: the next element of the DA on which `map()` operates
   - *obj*: the same parameter *obj* passed to `map()`
   - *wc*: the worker context, a persistent object local to each worker, where user can store and
	 access worker local dependencies.
 - *obj*: user provided data. Data will be passed to carrying serializable data from master to workers, obj is shared amongst mapper executions over each element of the DA

***NB:***
*wc is an optional object carrying the require method and able to store references and share them
between all transformations during application execution*

#### Exemple

The following program

```
var uc = require('ugrid').context();

function mapper(data, obj) { return data * obj.scaling }

var res = uc.parallelize([1, 2, 3, 4])
	.map(mapper, {scaling: 1.2})
	.collect();

res.on('data', console.log);
res.on('end', uc.end);
```

will display

```
1.2
2.4
3.6
4.8
```

### `DA.flatMap(flatMapper [, obj])`

Applies the provided mapper function to each element of the source DA and returns a new DA.

#### Arguments

   - *mapper*: a function, defined as function `flatMapper(data [[,obj] [, wc]])`, to be applied to each element of the dataset and which returns an array of elements
   - *obj*: optional object carrying serializable data from master to workers, obj is shared amongst mapper executions over each element of the DA

***NB:***
*wc is an optional object carrying the require method and able to store references and share them
between all transformations during application execution*

### ** Exemple:**

    #!/usr/local/bin/node --harmony
    'use strict';
    var ugrid = require('ugrid');
    ugrid.context(function(err, uc) {
	    if (err) {console.log(err); process.exit();}
	    function flatMapper(data, obj) {
	    var tmp = [];
	    for (var i = 0; i < obj.N; i++) tmp.push(data);
	    return tmp;
	    }
	    uc.parallelize([1, 2, 3, 4])
		    .flatMap(flatMapper, {N: 2})
		    .collect(function(err, res) {
			    if (err) {console.log(err); process.exit();}
		    console.log(res);
		    uc.end();
		});
    })

* ##**DA.mapValues(mapper [, obj])**

Applies the provided mapper function to the value of each [key, value] element of the source DA
and return a new DA containing elements defined as [key, mapper(value)], keeping the key
unchanged for each source element.

### ** Arguments:**

   - mapper: a function, defined as function mapper(data [[,obj] [, wc]]), to be applied to each element of the dataset and which returns a single element, conserving the key of source elements
   - obj: optional object carrying serializable data from master to workers, obj is shared amongst mapper executions over each element of the DA

***NB:***
*wc is an optional object carrying the require method and able to store references and share them
between all transformations during application execution*

### ** Exemple:**

    #!/usr/local/bin/node --harmony
    'use strict';
    var ugrid = require('ugrid');
    ugrid.context(function(err, uc) {
	    if (err) {console.log(err); process.exit();}
	    function valueMapper(data, obj) {
	    return data * obj.fact;
	    }
	    uc.parallelize([['hello', 1], ['world', 2]])
		    .mapValues(valueMapper, {fact: 2})
		    .collect(function(err, res) {
	    if (err) {console.log(err); process.exit();}
    console.log(res);
    uc.end();
	    });
    })

* ##**DA.flatMapValues(flatMapper [, obj])**

Applies the provided flatMapper function to the value of each [key, value] element of the source
DA and return a new DA containing elements defined as [key, mapper(value)], keeping the key
unchanged for each source element.

### ** Arguments:**

   - mapper: a function, defined as function flatMapper(data [[,obj] [, wc]]), to be applied to each element of the dataset and which returns an array of elements, conserving the key of source elements
   - obj: optional object carrying serializable data from master to workers, obj is shared amongst mapper executions over each element of the DA

***NB:***
*wc is an optional object carrying the require method and able to store references and share them
between all transformations during application execution*

### ** Exemple:**

    #!/usr/local/bin/node --harmony
    'use strict';
    var ugrid = require('ugrid');
    ugrid.context(function(err, uc) {
	    if (err) {console.log(err); process.exit();}
	    function valueFlatMapper(data, obj) {
	    var tmp = [];
	    for (var i = 0; i < obj.N; i++)
	    tmp.push(data * obj.fact);
	    return tmp;
	    }
	    uc.parallelize([['hello', 1], ['world', 2]])
		    .flatMapValues(valueFlatMapper, {N: 2, fact: 2})
		    .collect(function(err, res) {
	    if (err) {console.log(err); process.exit();}
    console.log(res);
	    uc.end();
	    });
    })

* ##**DA.filter(filter [, obj])**

Applies the provided filter function to each element of the source DA and returns a new DA
containing the elements that passed the test.

### ** Arguments:**

   - filter: a function, defined as function filter(data [[,obj] [, wc]]), to test each element of the dataset and which returns true or false
   - obj: optional object carrying serializable data from master to workers, obj is shared amongst mapper executions over each element of the DA

***NB:***
*wc is an optional object carrying the require method and able to store references and share them
between all transformations during application execution*

### ** Exemple:**

    #!/usr/local/bin/node --harmony
    'use strict';
    var ugrid = require('ugrid');
    ugrid.context(function(err, uc) {
	    if (err) {console.log(err); process.exit();}
	    function filter(data, obj) {
	    return data % obj.modulo;
	    }
	    uc.parallelize([1, 2, 3, 4])
		    .filter(filter, {modulo: 2})
		    .collect(function(err, res) {
	    if (err) {console.log(err); process.exit();}
    console.log(res);
	    uc.end();
	    });
    });

* Supported transformations, not yet documented
*    sample(withReplacement, frac, seed)
*    groupByKey()
*    reduceByKey()
*    union()
*    join(other)
*    leftOuterJoin(other)
*    rightOuterJoin(other)
*    coGroup(other)
*    crossProduct(other)
*    intersection(other)
*    subtract(other)
*    keys()
*    values()
*    Actions
*    aggregate()
*    reduce()
*    collect()
*    count()
*    forEach()
*    lookup(key)
*    countByValue()
*    countByKey()
