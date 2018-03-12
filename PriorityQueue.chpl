/*
	A priority queue implementation written in Chapel. The
	priority queue offers the standard operations via the 
	Collection's add' and 'remove' operations. Makes use of
	STM concepts such as read and write logs to allow
	concurrency, as well as Epoch-Based Reclamation and
	recycling memory (I.E RCUArray) to allow resizing
	of the data structure.

	The idea is this...

		Add(elt):
			var stm = manager.getDescriptor();
			// Insert...
			var insertSuccessful = false;
			var idx : int;
			while !insertSuccessful {
				try! {
					stm.begin();
					
					// Attempt to resize if too large
					idx = stm.read(this.size);
					var currData = stm.read(data);
					if idx >= stm.read(currData.cap) {
						this.expand();
					}
					
					data[idx] = elt;

					stm.commit();
					insertSuccessful = true;
				} catch ex : STMAbort {
					// Try again...	
				}
			}

			// Rebalance
			var rebalanceSuccessful = false;
			while !rebalanceSuccessful {
				try! {
					stm.begin();

					var arr = stm.read(data);
					if idx < stm.read(arr.cap) {
						var child = stm.read(arr[idx]);
						var parent = stm.read(arr[getParent(idx)]);

						// Heapify Up
						while idx != 0 && comparator(child, parent) == child {
							var tmp = stm.read(arr[idx]);
							stm.write(arr[idx], stm.read(arr[getParent(idx)]));
							stm.write(arr[getParent(idx)], tmp);

							idx = getParent(idx);
							child = stm.read(arr[idx]);
							if idx == 0 then break;
							parent = stm.read(arr[getParent(idx)]);
						}
					}
					
					stm.commit();
					rebalanceSuccessful = true;
				} catch ex : STMAbort {
					// Try again...
				}
			}
	
*/

use Collection;

class PriorityQueue : CollectionImpl {
	var comparator : func(eltType, eltType, eltType);
	var dom = {0..1024};
	var arr : [dom] eltType;
	var size : int;

	proc PriorityQueue(type eltType, comparator : func(eltType, eltType, eltType)) {
		this.comparator = comparator;
	}

	proc add(elt : eltType) : bool {
		on this {
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
					parent = arr[getParent(idx)];
				}
			}
		}
		return true;
	}

	// Implement Collection's 'remove' 
	proc remove() : (bool, eltType) {
		var retval : (bool, eltType);
      	on this {
      		if size > 0 {
				retval =  (true, arr[0]);
				arr[0] = arr[size - 1];
				size -= 1;

				heapify(0);
			}
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
		var curr = arr[idx];

		// left > current
		if size > l && comparator(curr, arr[l]) == arr[l] {
			curr = arr[l];
			idx = l;
		}

		// right > current
		if size > r && comparator(curr, arr[r]) == arr[r] {
			curr = arr[r];
			idx = r;
		}

		if idx != tmp {
			var swapTmp = arr[tmp];
			arr[tmp] = arr[idx];
			arr[idx] = swapTmp;

			heapify(idx);
		}
	}

	inline proc getParent(x:int) : int {
		return floor((x - 1) / 2);
	}

	inline proc getLeft(x:int) : int {
		return 2 * x + 1; 
	}

	inline proc getRight(x:int) : int {
		return 2 * x + 2;
	}
}

use Random;
use Sort;

proc main() { 
	const nElems = 1024 * 1024;
	var cmp = lambda(x:int, y:int) { return if x > y then x else y; };
	var pq = new PriorityQueue(int, cmp);

	// Generate random elems
	var rng = makeRandomStream(int);
	var arr : [1..nElems] int;
	rng.fillRandom(arr);

	// Test Collection's 'addBulk' utility method
	pq.addBulk(arr);

	// Test Collection's 'removeBulk'
	var sortedArr = pq.removeBulk(nElems);

	// Test result...
	var cmp2 = new ReverseComparator();
	assert(isSorted(sortedArr, cmp2));
	writeln("SUCCESS!");
}