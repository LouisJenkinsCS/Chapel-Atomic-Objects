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
use Sort;

class PriorityQueue : CollectionImpl {
	// Comparator record
	var cmp;
	var defaultSize : int;
	var dom = {0..-1};
	var arr : [dom] eltType;
	var size : int;

	proc PriorityQueue(type eltType, cmp = defaultComparator, defaultSize:int = 1024) {
		dom = {0..#defaultSize};
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
				while idx != 0 && chpl_compare(child, parent, cmp) == 1 {
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
		if size > l && chpl_compare(curr, arr[l], cmp) == -1 {
			curr = arr[l];
			idx = l;
		}

		// right > current
		if size > r && chpl_compare(curr, arr[r], cmp) == -1 {
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

	pragma "fn returns iterator"
	inline proc these() {
		return arr.these();
	}

	pragma "fn returns iterator"
	inline proc these(param tag : iterKind) {
		return arr.these(tag);
	}
}

use Random;


proc makePriorityQueue(arr : [] ?eltType, cmp=defaultComparator) {
	var newArr = reshape(arr, {0..(arr.domain.high - arr.domain.low)});
	heapSort(newArr, cmp);
	
	var pq = new PriorityQueue(eltType, cmp);
	pq.dom = newArr.domain;
	pq.arr = newArr;

	return pq;
}



proc main() { 
	const nElems = 16;

	// Generate random elems
	var rng = makeRandomStream(int);
	var arr : [1..nElems] int;
	rng.fillRandom(arr);

	var pq = makePriorityQueue(arr);

	// Test Collection's 'addBulk'
	pq.addBulk(rng.iterate({1..nElems}));

	// Test Collection's 'removeBulk'
	var sortedArr = pq.removeBulk(nElems);

	assert(isSorted(sortedArr, reverseComparator));
	writeln("SUCCESS!");
}