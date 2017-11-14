# Transformations

## map(func, args)
Return a new distributed dataset formed by passing each element of the source through a function func.

## union(array)
Return a new dataset that contains the union of the elements in the source dataset and the argument.

## reduceByKey(key, func, [initValue])
When called on a dataset of tuples (..., (Ki, Vi), ...), returns a dataset of (key, V) pairs where the values for each key are aggregated using the given reduce function func, which must be of type (V,V) => V. Initial value of reduction operation can be set optionally.

# Actions

## reduce(func) 
Aggregate the elements of the dataset using a function func (which takes two arguments and returns one). The function should be commutative and associative so that it can be computed correctly in parallel.

## collect()
Return all the elements of the dataset as an array at the driver program. This is usually useful after a filter or other operation that returns a sufficiently small subset of the data.

## count()  
Return the number of elements in the dataset.

## sample(fraction)

## takeSample(num)
Return an array with a random sample of num elements of the dataset, with or without replacement, optionally pre-specifying a random number generator seed.
