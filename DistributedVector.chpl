use Collection;
use BitIndexer;

/*
	DistributedVector is a scalable, resizable, indexable, totally-ordered, and distributed
	container. The basic idea is this...

	1)	For each instance, we allocate enough memory on each node to fulfill a request.
	2)	When indexing into the vector, it will cycle through each node's memory so that
		we maintain a balance of computational, memory, and network workload.
	3)	When indexing out of bounds for the first time, we acquire the RCU writer lock
		and create a copy of our instance, copying the pointers to the old memory and 
		appending/changing the metadata to also keep track of new, and install that. If
		we fail to acquire the writer lock, we wait for it to be released and check if
		there is enough space.

	The above allows us to ensure that those indexing already-allocated spots can continue
	to do so without issue, as they will be writing to the same pointers in memory whether
	they are using the new or old (transient state) version. 
*/

config param DistVectorChunkSize = 1024;

record DistVector {
	type eltType;

	// Privatization id
	var pid : int = -1;

	proc DistVector(type eltType) {
		pid = (new DistVectorImpl(eltType, new DistVectorWriterLock())).pid;
	}

	proc ~DistVector() {
		// coforall loc in Locales do on loc do delete _value;
	}

	proc _value {
		if pid == -1 {
			halt("DistVector is uninitialized...");
		}

		return chpl_getPrivatizedCopy(DistVectorImpl(eltType), pid);
	}

	forwarding _value;
}

class DistVectorImpl : CollectionImpl {
	// Privatization id
	var pid : int;

	// We maintain two separate RCU instances as the first dimension which we alternate
	// between. Each reader will increment the read counter and then obtain the appropriate
	// index for the first dimension. While the read counter is non-zero, it is safe to access
	// the current index as the writer will wait for it to reach zero (TODO: Implement some kind
	// of Task-Local-Storage so we bound the wait for writers). Writers will safely copy data from
	// the current instance into the reserved instance along with other needed modifications, then
	// will update the current index and await read counter to hit 0. By waiting for it to hit 0, we
	// ensure that no other writer to will modify an instance another reader is using while alternating.
	// Readers will write to the same already-existing containers in either container.
	var instances : [0..1] DistVectorInstance(eltType);
	// Current RCU instance index
	var instanceIdx : atomic int;

	// Current number of readers for any given index (readCount[0..1] for instances[0..1])
	// When a reader acquires access to one of the instances, it must notify to the writer
	// that it must not continue perform unsafe writes to that given instance.
	var readCount : [0..1] atomic int;

	// Lock all writers must acquire; it is allocated on a single node to ensure they all
	// may linearize on it.
	var writeLock : DistVectorWriterLock;
	var nextLocaleAlloc = 1;

	// Requires reader or writer privilege
	proc getSlot(slotIdx) {
		var idx = instanceIdx.read();
		return instances[idx][slotIdx];
	}

	// Acquires read-access to the current instance. The index of the instance
	// is returned so that the reader may release read-access for the appropriate instance.
	proc acquireRead() : int {
		var idx = instanceIdx.read();
		readCount[idx].add(1);
		return idx;
	}

	// Releases read-access to the current instance. The index must be the same
	// that is obtained from the 'acquireRead' read-barrier 
	proc releaseRead(idx : int) {
		var cnt = readCount[idx].fetchAndSub(1);
	}

	proc acquireWrite() {
		writeLock.lock();
	}

	proc releaseWrite() {
		writeLock.unlock();
	}

	// NOTE: Check whether this function returns privatized data only
	inline proc currRC : ref {
		var idx = instanceIdx.read();
		return readCount[idx];
	}

	inline proc otherRC : ref {
		var idx = !instanceIdx.read();
		return readCount[idx];
	}

	inline proc currInstance : ref {
		var idx = instanceIdx.read();
		return instances[idx];
	}

	// Requires writer privilege
	inline proc switchInstance() {
		// Update all instances...
		coforall loc in Locales do on loc {
			var _this = getPrivatizedThis;
			var idx = _this.instanceIdx.write(!_this.instanceIdx.read());
		}
	}

	inline proc waitForReaders(idx : int) {
		coforall loc in Locales do on loc {
			var _this = getPrivatizedThis;
			_this.readCount[idx].waitFor(0);
		}
	}

	proc allocateSlot(slotIdx, nCells) {
		// Allocate a slot
		var newSlot = new DistVectorSlot(eltType);
		newSlot.dom = {0..nCells};
		for i in 0 .. nCells {
			on Locales[(nextLocaleAlloc + i) % LocaleSpace.size] {
				newSlot.cells[i] = new DistVectorMutableSingleton(eltType);
			}
		}
		
		coforall loc in Locales do on loc {
			var _this = getPrivatizedThis;
			_this.nextLocaleAlloc = _this.nextLocaleAlloc + 1;
			var oldIdx = _this.instanceIdx.read();
    		var newIdx : int;
    		if oldIdx == 1 then newIdx = 2;
    		else newIdx = 1;

    		// Read and Copy old data to new...
    		ref oldInstance = _this.instances[oldIdx];
    		ref newInstance = _this.instances[newIdx];
    		newInstance = oldInstance;

    		// Update copy with new data...
    		newInstance[slotIdx] = newSlot;
		}
	}

	proc DistVectorImpl(type eltType, lock : DistVectorWriterLock) {
		instanceIdx.write(1);

		writeLock = lock;
		pid = _newPrivatizedClass(this);
	}

 	proc DistVectorImpl(other, privData, type eltType = other.eltType) {
		instanceIdx.write(1);

		this.writeLock = other.writeLock;
    }

    pragma "no doc"
    proc dsiPrivatize(privData) {
        return new DistVectorImpl(this, privData);
    }

    pragma "no doc"
    proc dsiGetPrivatizeData() {
      return pid;
    }

    pragma "no doc"
    inline proc getPrivatizedThis {
      return chpl_getPrivatizedCopy(this.type, pid);
    }

    proc add(elt: eltType): bool {
    	// As it is possible for other insertions to update
    	while true {
    		var rcIdx = acquireRead();
    		var instance = currInstance;
    		var slot = instance.slots[instance.slots.size - 1];
    		ref cachedUsed = slot.used;
    		
    		// Check number of used space (blocking read)
    		var used = cachedUsed;
    		if used >= DistVectorChunkSize {
    			// Check if instance is the same; if so, we need to become
    			// the RCU writer. Note that if there was another RCU writer,
    			// we would still be blocked on reading the 'cache' sync variable.
    			if instance == currInstance {
    				releaseRead(rcIdx);
    				acquireWrite();

    				// To ensure that data is distributed, we cycle between each node to determine
    				// which will allocate the next chunk. For example, given 2 nodes and 4096 elements
    				// with a chunk size of 1024, we will have indices 0..1023 and 2048 ... 3071 mapping to node 1
    				// and 1024 .. 2047 and 3072 .. 4091 mapping to node 2, similar to a cyclic block distribution.
    				var instIdx = instanceIdx.read();
    				var newSlot : DistVectorSlot(eltType) = nil;
    				var locidToAlloc = (nextLocaleAlloc + 1) % numLocales;
    				on Locales[locidToAlloc] do newSlot = new DistVectorSlot(eltType);

    				// We append the allocated slot to the unused instance and set it as the current (cross-node update)
    				coforall loc in Locales do on loc {
    					var _this = getPrivatizedThis;
    					_this.nextLocaleAlloc = locidToAlloc;
    					var newInstIdx = !_this.instanceIdx.read();
    					var newInstance = _this.instances[newInstIdx];
    					newInstance.slots.push_back(newSlot);
    					_this.instanceIdx.write(newInstIdx);
    				}

    				// Unblock readers and wait for them to finish.
    				cacheUsed = used;
    				waitForReaders(instIdx);
    			} else {
    				// Fill sync variable and loop again as current instance was updated...
    				cachedUsed = used;
    				releaseRead(rcIdx);
    				continue;
    			}
    		} else {
    			// There is enough room for our operation so we claim our space
    			// and fill sync variable for others.
    			slot.elems[used + 1] = elt;
    			cachedUsed = used + 1;
    		}

    		releaseRead(rcIdx);
    		return true;
    	}
    }

    // Indexes into the distributed vector
    proc this(idx : int) ref {
    	var rcIdx = acquireRead();
    	var instance = currInstance;

    	// If the index is currently allocated, then we simply
    	// fetch the actual element being referenced.
    	if instance.isAllocated(idx) {
    		ref elt = instance[idx];
	    	releaseRead(rcIdx);
	    	return elt;
    	}
    	releaseRead(rcIdx);
    	halt("idx #", idx, " is out of bounds...");
    }

    proc contains(elt: eltType): bool {
    	var rcIdx = acquireRead();
    	var instance = currInstance;
    	var found : atomic bool;
    	
    	forall slot in instance.slots {
    		if !found.read() {
    			on slot do for eltIdx in 1 .. slot.used.readXX() {
    				if elt == slot[eltIdx] {
    					found.write(true);
    					break;
    				}
    			}
    		}
    	}

    	releaseRead(rcIdx);
    	return found.read();
    }
}

// Each slot contains DistVectorChunkSize contiguous chunks of data.
// Since the entire chunk is allocated at one time, we keep track of
// portion of the chunk that is actually in use via a sync variable, 'used'.
// Readers can safely read 'used' using non-blocking approaches, but writers
// must first acquire 'used' as full -> empty to retrieve both the number
// of chunk used and proper mutual exclusion; they then make their changes 
// to the next unused chunk and release 'used' as empty -> full.
// This approach provides a second and less costly performance penalty for
// writers, and yet still allow for readers to be without cost. 
class DistVectorSlot {
	type eltType;
	var elems : (DistVectorChunkSize * eltType);
	var used : sync int = 0;
}

record DistVectorInstance {
	type eltType;
	var slots : [{0..0}] DistVectorSlot(eltType);

	// Determines if the requested index currently exists, which is true if and only if
	// the slot is allocated and that the position in the slot requested is in use.
	proc isAllocated(idx : int) {
		var slotIdx = idx / DistVectorChunkSize;
		var elemIdx = (idx % DistVectorChunkSize) + 1;
		return slots.size > slotIdx && slots[slotIdx].used.readXX() >= elemIdx;
	}

	proc this(idx : int) : ref {
		var slotIdx = idx / DistVectorChunkSize;
		var elemIdx = (idx % DistVectorChunkSize) + 1;
		return slots[slotIdx][elemIdx];
	}
}

class DistVectorWriterLock {
	var lock$ : sync bool;

	inline proc lock() {
		lock$ = true;
	}

	inline proc unlock() {
		lock$;
	}
}

