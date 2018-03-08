use Time;
use Collection;
use LocalAtomicObject;

config param MAX_CCSYNCH_REQUESTS = 64;
config param PRIORITY_QUEUE_DEFAULT_SIZE = 1024;
param PRIORITY_QUEUE_ADD = 0;
param PRIORITY_QUEUE_REMOVE = 1;

/*
	A priority queue implementation written in Chapel. The
	priority queue offers the standard operations via the 
	Collection's add' and 'remove' operations. Makes use
	of the CC-Synch algorithm to perform flat-combining to
	allow scalable mutual exclusion.
*/

class CCSynchNode {
	type eltType;

	// Result of operation
    var elt : eltType;
    var op : int;
    var status : int;

    // If wait is false, we spin
    // If wait is true, but completed is false, we are the new combiner thread
    // If wait is true and completed is true, we are done and can exit
    var wait : atomic bool;
    var completed : bool;

    // Next in the waitlist
    var next : LocalAtomicObject(CCSynchNode(eltType));
}

class PriorityQueue : CollectionImpl {
	var comparator : func(eltType, eltType, eltType);
	var dom = {0..#PRIORITY_QUEUE_DEFAULT_SIZE};
	var arr : [dom] eltType;
	var size : int;

	var ccWaitList : LocalAtomicObject(CCSynchNode(eltType));

	proc PriorityQueue(type eltType, comparator : func(eltType, eltType, eltType)) {
		this.comparator = comparator;

		// Create dummy node
		ccWaitList.write(new CCSynchNode(eltType));
	}

	proc add(elt : eltType) : bool {
		on this {
			var resultNode = doCCSynch(PRIORITY_QUEUE_ADD, elt);
			delete resultNode;
		}
		return true;
	}

	proc remove() : (bool, eltType) {
		var retval : (bool, eltType);
      	on this {
      		var resultNode = doCCSynch(PRIORITY_QUEUE_REMOVE);
      		retval = (resultNode.status == 1, resultNode.elt);
      		delete resultNode;
      	}
      	return retval;
	}

	// Perform CCSynch algorithm and get result
	proc doCCSynch(op : int, elt : eltType = _defaultOf(eltType)) : CCSynchNode(eltType) {
		var counter = 0;
      	var nextNode = new CCSynchNode(eltType);
      	nextNode.wait.write(true);
      	nextNode.completed = false;

      	// Register our dummy node so that the next task can add theirs safely,
      	// then fill out the node we assigned to use
      	var currNode = ccWaitList.exchange(nextNode);
      	currNode.op = op;
      	currNode.elt = elt;
      	currNode.next.write(nextNode);

      	// Spin until we are finished...
      	currNode.wait.waitFor(false);

      	// If our operation is marked complete, we may safely reclaim it, as it is no
      	// longer being touched by the combiner thread
      	if currNode.completed {
        	return currNode;
      	}

      	// If we are not marked as complete, we *are* the combiner thread
      	var tmpNode = currNode;
      	var tmpNodeNext : CCSynchNode(eltType);

      	while (tmpNode.next.read() != nil && counter < MAX_CCSYNCH_REQUESTS) {
        	counter = counter + 1;
        	// Note: Ensures that we do not touch the current node after it is freed
        	// by the owning thread...
        	tmpNodeNext = tmpNode.next.read();

        	// Process
        	select tmpNode.op {
        		when PRIORITY_QUEUE_ADD {
        			var idx = size;
        			
        			// Resize if needed
        			if idx >= dom.last {
        				dom = {0..(((dom.last * 1.5) : int) - 1)};
        			}

        			// Insert
        			arr[idx] = tmpNode.elt;
        			size += 1;

        			// Rebalance
        			if idx > 0 {
        				var child = arr[idx];
        				var parent = arr[getParent(idx)];

        				// Heapify Up
        				while idx != 0 && comparator(child, parent) == child {
        					var tmp = arr[idx];
        					arr[idx] = arr[getParent(idx)];
        					arr[getParent(idx)] = tmp;

        					idx = getParent(idx);
        					child = arr[idx];
        					if idx == 0 then break;
        					parent = arr[getParent(idx)];
        				}
        			}
        		}
        		when PRIORITY_QUEUE_REMOVE {
        			if size != 0 {
        				var tmp = arr[0];
        				arr[0] = arr[size - 1];
        				arr[size - 1] = tmp;
        				size -= 1;

        				heapify(0);
        				tmpNode.status = 1;
        				tmpNode.elt = tmp;
        			}
        		}
        	}

        	// We are done with this one... Note that this uses an acquire barrier so
        	// that the owning task sees it as completed before wait is no longer true.
        	tmpNode.completed = true;
        	tmpNode.wait.write(false);

        	tmpNode = tmpNodeNext;
      	}

      	// At this point, it means one thing: Either we are on the dummy node, on which
      	// case nothing happens, or we exceeded the number of requests we can do at once,
      	// meaning we wake up the next thread as the combiner.
      	tmpNode.wait.write(false);
      	return currNode;
	}

	proc heapify(_idx : int) {
		var idx = _idx;
		if size <= 1 {
			return;
		}

		var l = getLeft(idx);
		var r = getRight(idx);
		var tmp = idx;

		// Out of bounds
		if l >= size || r >= size {
			return;
		}

		var left = arr[l];
		var right = arr[r];
		var curr = arr[idx];

		// left > current
		if comparator(curr, left) == left {
			curr = left;
			idx = l;
		}

		// right > current
		if comparator(curr, right) == right {
			curr = right;
			idx = r;
		}

		if idx != tmp {
			var swapTmp = arr[tmp];
			arr[tmp] = arr[idx];
			arr[idx] = swapTmp;

			heapify(idx);
		}
	}

	inline proc getLeft(x) {
		return 2 * x + 1; 
	}

	inline proc getRight(x) {
		return 2 * x + 2;
	}

	inline proc getParent(x) {
		return (x - 1) / 2;
	}
}

config var weakScaling = false;

proc main() {
	const nTrials = 1;
	const nOperations = 1024 * 1024; 
	var pq = new PriorityQueue(int, lambda(x:int, y:int) { return if x > y then x else y; });
	var t : Timer();

	for maxTaskPar in 1..here.maxTaskPar {
		if maxTaskPar != 1 && maxTaskPar % 2 != 0 then continue;
		var trialTimes : [0..#nTrials] real;
		for trial in 0..#nTrials {
			t.start();
			// Concurrent Add Phase
			coforall tid in 0..#maxTaskPar {
				var iterations = nOperations / maxTaskPar; 
				var start = iterations * tid;
				var end = iterations * (tid + 1);
				if !weakScaling then for i in start..#end do pq.add(i);
				else for i in 0..#nOperations do pq.add(i);
			}
			// Concurrent Remove Phase
			coforall tid in 0..#maxTaskPar {
				var iterations = nOperations / maxTaskPar; 
				var start = iterations * tid;
				var end = iterations * (tid + 1);
				if !weakScaling then for i in start..#end do pq.remove();
				else for i in 0..#nOperations do pq.remove();
			}
			t.stop();
			trialTimes[trial] = t.elapsed();
			t.clear();
		}

		writeln("[", maxTaskPar, " Threads]: ", 
			(if weakScaling then nOperations * maxTaskPar else nOperations) / (+ reduce trialTimes / nTrials**2));
	}
}