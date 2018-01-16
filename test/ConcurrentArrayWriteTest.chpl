use ConcurrentArray;
use Time;

var array : ConcurrentArray(int);
var sz : atomic int;
var done : atomic bool;

// One writer will repeatedly expand the array
sz.write(ConcurrentArrayChunkSize);
array.expand(ConcurrentArrayChunkSize);

begin {
	for i in 1 .. #(ConcurrentArrayChunkSize / 64) {
		array.expand(ConcurrentArrayChunkSize);
		sz.write(ConcurrentArrayChunkSize * (i+1));
		writeln("Expanded to ", sz.read());
		sleep(1);
	}
	done.write(true);
}

// Many readers will continuously index randomly into the array...
coforall tid in 1..#here.maxTaskPar {
	while !done.read() {
		var _sz = sz.read();
		for i in 0 .. #_sz {
			array[i] = i;
		}
		chpl_task_yield();
	}
}
