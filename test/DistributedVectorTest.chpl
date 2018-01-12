use DistributedVector;

var vector : DistVector(int);

// Initially we have a single chunk of size 'DistVectorChunkSize'; fill those with deterministic values
forall ix in 0 .. #DistVectorChunkSize {
	vector[ix] = ix;
}

// Expand and test until we have DistVectorChunkSize^2
for i in 1 .. #(DistVectorChunkSize / 64) {
	vector.expand(DistVectorChunkSize);

	// Fill-in the part of the vector we have just allocated
	forall ix in 0 .. #DistVectorChunkSize {
		var idx = ix + (DistVectorChunkSize * i);
		vector[idx] = idx;
	}

	// Test that all elements are still their expected values...
	forall ix in 0 .. #vector.size {
		assert(vector[ix] == ix);
	}

	writeln("Passed: ", vector.size);
}
