use Time;
use Collection;
use LocalAtomicObject;


/*
	A priority queue implementation written in Chapel. The
	priority queue offers the standard operations via the 
	Collection's add' and 'remove' operations. Makes use of
	STM concepts such as read and write logs to allow
	concurrency, as well as Epoch-Based Reclamation and
	recycling memory (I.E RCUArray) to allow resizing
	of the data structure.
*/

config param PRIORITY_QUEUE_BLOCK_SIZE = 1024;

class ArrayBlock {
	type eltType;
	var arr : [0..#PRIORITY_QUEUE_BLOCK_SIZE] eltType;

	proc this(idx) ref {
		if (idx >= PRIORITY_QUEUE_BLOCK_SIZE) {
			halt(idx, " >= ", PRIORITY_QUEUE_BLOCK_SIZE);
		}

		return arr[idx];
	}
}

class ArrayImpl {
	type eltType;
	var dom = {0..-1};
	var block : [dom] ArrayBlock;

	proc this(idx) ref {
		var blockIdx = idx / PRIORITY_QUEUE_BLOCK_SIZE;
		var elemIdx = idx % PRIORITY_QUEUE_BLOCK_SIZE;

		if blockIdx >= dom.high {
			halt(idx, " >= ", dom.high * PRIORITY_QUEUE_BLOCK_SIZE);
		}

		return block[blockIdx];
	} 
}

class PriorityQueue : CollectionImpl {
	var comparator : func(eltType, eltType, eltType);
	var data : ArrayImpl;
	var size : int;

	// Lock used to resize
	var lock$ : sync bool;

	proc PriorityQueue(type eltType, comparator : func(eltType, eltType, eltType)) {
		this.comparator = comparator;
		
		// Initialize with one block
		this.data = new ArrayImpl(eltType);
		this.data.dom = {0..0};
		this.data.block[0] = new ArrayBlock(eltType);
	}

	// Parllel-safe
	proc expand() {
		lock$ = true;

		// Create new Array that recycles old memory
		var newData = new ArrayImpl();
		newData.dom = {0..data.dom.high + 1};
		newData[0..data.dom.high] = data.block;
		newData[newData.dom.high] = new ArrayBlock();

		// Update as current
		// TODO: Manage deletion of previous one
		data = newData;

		lock$;
	}

	/*
		Code for add...

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

	*/
	proc add(elt : eltType) : bool {
		on this {
			// TODO
		}
		return true;
	}

	/*
		Code for remove...

			if size != 0 {
				var tmp = arr[0];
				arr[0] = arr[size - 1];
				arr[size - 1] = tmp;
				size -= 1;

				heapify(0);
				tmpNode.status = 1;
				tmpNode.elt = tmp;
			}
	*/
	proc remove() : (bool, eltType) {
		var retval : (bool, eltType);
      	on this {
      		// TODO
      	}
      	return retval;
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

	proc syncAdd(elt : eltType) {
		lock$ = true;
		var idx = size;
        			
		// Resize if needed
		if idx >= dom.last {
			dom = {0..(((dom.last * 1.5) : int) - 1)};
		}

		// Insert
		arr[idx] = elt;
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

		lock$;
	}

	proc syncRemove() {

		lock$ =true;

		if size != 0 {
			var tmp = arr[0];
			arr[0] = arr[size - 1];
			arr[size - 1] = tmp;
			size -= 1;

			heapify(0);
		}

		lock$;
	}
}

config var weakScaling = false;
config var nTrials = 4;
config var nOperations = 1024 * 1024;

proc main() { 
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

		writeln("CCSynch ~ [", maxTaskPar, " Threads]: ", 
			(if weakScaling then 2 * nOperations * maxTaskPar else 2 * nOperations) / (+ reduce trialTimes / nTrials**2));
	}

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
				if !weakScaling then for i in start..#end do pq.syncAdd(i);
				else for i in 0..#nOperations do pq.syncAdd(i);
			}
			// Concurrent Remove Phase
			coforall tid in 0..#maxTaskPar {
				var iterations = nOperations / maxTaskPar; 
				var start = iterations * tid;
				var end = iterations * (tid + 1);
				if !weakScaling then for i in start..#end do pq.syncRemove();
				else for i in 0..#nOperations do pq.syncRemove();
			}
			t.stop();
			trialTimes[trial] = t.elapsed();
			t.clear();
		}

		writeln("Sync ~ [", maxTaskPar, " Threads]: ", 
			(if weakScaling then 2 * nOperations * maxTaskPar else 2 * nOperations) / (+ reduce trialTimes / nTrials**2));
	}
}