use DistributedVector;
use Time;

var vector : DistVector(int);
var sz : atomic int;
var done : atomic bool;

// One writer will repeatedly expand the vector
sz.write(DistVectorChunkSize);
begin {
	for i in 1 .. #(DistVectorChunkSize / 64) {
		vector.expand(DistVectorChunkSize);
		sz.write(DistVectorChunkSize * (i+1));
		writeln("Expanded to ", sz.read());
		sleep(1);
	}
	done.write(true);
}

// Many readers will continuously index randomly into the vector...
coforall tid in 1..#here.maxTaskPar {
	while !done.read() {
		var _sz = sz.read();
		for i in 0 .. #_sz {
			vector[i] = i;
		}
		chpl_task_yield();
	}
}
