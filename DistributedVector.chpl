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
	var instances : [1..2] [1..64] DistVectorSlot(eltType);
	// Current RCU instance index
	var instanceIdx : atomic int;
	// Current number of readers (TODO: Need Task-Local-Storage)
	var readCount : atomic int;
	// Lock all writers must acquire; it is allocated on a single node to ensure they all
	// may linearize on it.
	var writeLock : DistVectorWriterLock;
	var nextLocaleAlloc = 1;

	// Requires reader or writer privilege
	proc getSlot(slotIdx) {
		var idx = instanceIdx.read();
		return instances[idx][slotIdx];
	}

	proc acquireRead() {
		readCount.add(1);
	}

	proc releaseRead() {
		readCount.sub(1);
	}

	proc acquireWrite() {
		writeLock.lock();
	}

	proc releaseWrite() {
		writeLock.unlock();
	}

	// Requires writer privilege
	inline proc switchInstance() {
		// Update all instances...
		coforall loc in Locales do on loc {
			var _this = getPrivatizedThis;
			var idx = _this.instanceIdx.read();
			if idx == 1 then _this.instanceIdx.write(2);
			else _this.instanceIdx.write(1);
		}
	}

	inline proc waitForReaders() {
		coforall loc in Locales do on loc {
			var _this = getPrivatizedThis;
			_this.readCount.waitFor(0);
		}
	}

	proc allocateSlot(slotIdx, nCells) {
		// Allocate a slot
		// writeln("Allocating for slot #", slotIdx);
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

    inline proc isSlotAllocated(slotIdx) {
    	return getSlot[slotIdx].dom != {0..-1};
    }

    proc this(idx : int) ref {
    	var (slotIdx, cellIdx) = getIndex(idx);
    	var slot : DistVectorMutableSingleton(eltType);

    	// Fast path
    	acquireRead();
    	if isSlotAllocated(slotIdx) {
    		slot = getSlot(slotIdx).cells[cellIdx];
	    	releaseRead();
	    	slot.used = true;
	    	return slot.slot;
    	}
    	releaseRead();

    	// Slow Path
    	while true {
	    	acquireRead();
	    	if !isSlotAllocated(slotIdx) {
	    		releaseRead();
	    		acquireWrite();

	    		// Double-check
	    		if !isSlotAllocated(slotIdx) {
	    			allocateSlot(slotIdx, ((1 << slotIdx - 1) - 1));
	    			switchInstance();
	    			slot = getSlot(slotIdx).cells[cellIdx];

		    		waitForReaders();
		    		releaseWrite();
		    		break;
	    		}

	    		// Already filled by someone else
	    		slot = getSlot(slotIdx).cells[cellIdx];
	    		releaseWrite();
	    		break;
	    	}

	    	// Exists...
	    	slot = getSlot(slotIdx).cells[cellIdx];
	    	releaseRead();
	    	break;
    	}

    	slot.used = true;
    	return slot.slot;
    }

    proc contains(elt: eltType): bool {
    	acquireRead();

    	var (slotIdx, cellIdx) = getIndex(elt);
    	var retval = isSlotAllocated(slotIdx) && getSlot(slotIdx).cells[cellIdx].used;
    	
    	releaseRead();
    	return retval;
    }
}

record DistVectorSlot {
	type eltType;
	var dom = {0..-1};
	var cells : [dom] DistVectorMutableSingleton(eltType);

	inline proc isAllocated(slotIdx) {
		return cells[slotIdx] != nil;
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

class DistVectorMutableSingleton {
	type eltType;
	var slot : eltType;
	var used : bool;
}

