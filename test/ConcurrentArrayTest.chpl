use ConcurrentArray;

var array = new ConcurrentArray(int);
array.expand(ConcurrentArrayChunkSize);

// Initially we have a single chunk of size 'DistVectorChunkSize'; fill those with deterministic values
forall ix in 0 .. #ConcurrentArrayChunkSize {
	array[ix] = ix;
}

// Expand and test until we have DistVectorChunkSize^2
for i in 1 .. #(ConcurrentArrayChunkSize / 64) {
	array.expand(ConcurrentArrayChunkSize);

	// Fill-in the part of the array we have just allocated
	forall ix in 0 .. #ConcurrentArrayChunkSize {
		var idx = ix + (ConcurrentArrayChunkSize * i);
		array[idx] = idx;
	}

	// Test that all elements are still their expected values...
	forall ix in 0 .. #array.size {
		assert(array[ix] == ix);
	}

	writeln("Passed: ", array.size);
}
