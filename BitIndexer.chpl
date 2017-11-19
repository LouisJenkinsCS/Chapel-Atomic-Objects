// Algorithm to decode an integer into the index of the first dimension (most-signicant bit)
// and the index of the second dimension (being the remainder), allowing each dimension to
// be the size of a power of two, and be determined in O(1) time.
module BitIndexer {

 	inline proc getIndex(n) : (int, int) {
    	// If n is 1, then there is only one slot
	    if n == 1 {
	      return (1,0);
	    }

	    // Find the most significant bit
	    var bit = 63;
	    while bit > 0 {
	      if n & (1 << bit) != 0 then break;
	      bit = bit - 1;
	    }

	    return (bit + 1, n & ((1 << bit) - 1));
  	}

}