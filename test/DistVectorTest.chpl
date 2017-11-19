use DistributedVector;
use Random;

proc main() {
	var vec = new DistVector(int);
	coforall tid in 0 .. #here.maxTaskPar {
		var randStream = makeRandomStream(uint);
		for 1 .. 1024 {
			var idx = (randStream.getNext() % 1024) : int;
			if !vec.contains(idx) {
				vec[idx] = idx;
				writeln(tid, ": Allocating #", idx);
			} else {
				assert(vec[idx] == idx);
				writeln(tid, ": Passed check for #", idx);
			}
		}
	}

	writeln(vec);
}