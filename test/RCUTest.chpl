use RCU;
use CyclicDist;
use Random;
use Time;

class NonResizableArray {
	var space = {0..31};
	var dom = space dmapped Cyclic(startIdx=0);
	var arr : [dom] int;
}

class ResizableArray {
	var top$ : sync int = 0;
	var rcu : RCU(NonResizableArray);
		
	proc ResizableArray() {
		rcu.update(new NonResizableArray());
	}

	proc push(elt) {
		var _top = top$;

		// Attempt to push if we have enough space...
		rcu.acquireReadBarrier();
		var isFull : bool;
		var arr = rcu.read();
		if _top <= arr.dom.high {
			arr.arr[_top] = elt;
		} else {
			isFull = true;
		}
		rcu.releaseReadBarrier();

		// If we are full, we need to allocate more space. We must move all data from the old into the new.
		// Note that the 'arr' could have changed since we relinquished reader status, so we must read it again...
		if isFull {
			rcu.acquireWriteBarrier();
			arr = rcu.read();

			// If someone else has resized it already and we have enough space, then we are good.
			if _top <= arr.dom.high {
				arr.arr[_top] = elt;
			}
			// Otherwise, we resize...
			else {
				var newArr = new NonResizableArray();
				newArr.dom = {0..#(arr.dom.size * 2)};
				newArr.arr[0..#arr.dom.size] = arr.arr[0..#arr.dom.size];
				newArr.arr[_top] = elt;
				rcu.update(newArr);
				delete arr;
			}

			rcu.releaseWriteBarrier();
		}

		top$ = _top + 1;
	}

	// A simple test to ensure that all elements inserted are distinct and are below the maximum amount.
	// We perform a reduction over the array; if the array is unsafely deleted or touched while resizing,
	// then we will get undefined behavior and likely fail (or worse: crash...). 
	proc test() {
		rcu.acquireReadBarrier();

		var arr = rcu.read();
		var finalTotal = ((numLocales * 100) * ((numLocales * 100) + 1)) / 2;
		var total = + reduce arr.arr;
		assert(total <= finalTotal);
		rcu.releaseReadBarrier();
	}
}

var arr = new ResizableArray();

var keepAlive : atomic bool;
keepAlive.write(true);

begin {
	var randStream = makeRandomStream(real);
	while keepAlive.read() {
		arr.test();

		sleep(randStream.getNext(), TimeUnits.milliseconds);
	}
}

coforall loc in Locales do on loc {
	var randStream = makeRandomStream(real);
	forall i in ((here.id * 100) + 1) .. ((here.id + 1) * 100) {
		arr.push(i);
		sleep(randStream.getNext(), TimeUnits.milliseconds);
	}
}

keepAlive.write(false);

writeln("SUCCESS!");