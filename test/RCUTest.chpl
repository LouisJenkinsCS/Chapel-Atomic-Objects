use RCU;
use Time;

class NonResizableArray {
	var dom : {0..0};
	var arr : [dom] int;
}

class ResizableArray {
	var top$ : sync int = 0;
	var rcu : RCU(NonResizableArray);
		
	proc ResizableArray() {
		rcu.writeBarrier(lambda(arr : NonResizableArray) : NonResizableArray { return new NonResizableArray(); });
	}

	proc push(elt) {
		var _top = top$;

		// Attempt to push if we have enough space... note that while we are doing this
		// operation, indexing operations are allowed to proceed.
		var isFull : bool;
		rcu.readBarrier(lambda(arr : NonResizableArray) {
			if _top > arr.dom.high {
				arr[_top] = elt;
			} else {
				isFull = true;
			}
		});

		// If we are full, we need to allocate more space. We must move all data from the old into the new.
		rcu.writeBarrier(lambda(arr : NonResizableArray) : NonResizableArray {
			var newArr = new NonResizableArray();
			newArr.dom = {0..#(arr.dom.size * 2)};
			newArr.arr[0..#arr.dom.size] = arr.arr[0..#arr.dom.size];
			newArr.arr[_top] = elt;
			return newArr;
		});

		top$ = _top + 1;
	}

	proc write() {
		rcu.readBarrier(lambda(arr : NonResizableArray) {
			writeln(arr.arr);
		});
	}
}

var arr = new ResizableArray();

var keepAlive : atomic bool;
keepAlive.write(true);

begin {
	while keepAlive.read() {
		arr.write();
		sleep(2);
	}
}

forall i in 1 .. 1000 {
	arr.push(i);
	sleep(1);
}

keepAlive.write(false);
