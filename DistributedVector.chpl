use Collection;

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
		readCount[idx].sub(1);
	}

	proc acquireWrite() {
		writeLock.lock();
	}

	proc releaseWrite() {
		writeLock.unlock();
	}

	inline proc currInstance ref {
		var idx = instanceIdx.read();
		return instances[idx];
	}

	// Requires writer privilege
	inline proc switchInstance() {
		// Update all instances...
		coforall loc in Locales do on loc {
			var _this = getPrivatizedThis;
			var idx = _this.instanceIdx.write(!(_this.instanceIdx.read()));
		}
	}

	inline proc waitForReaders(idx : int) {
		coforall loc in Locales do on loc {
			var _this = getPrivatizedThis;
			_this.readCount[idx].waitFor(0);
		}
	}

	proc DistVectorImpl(type eltType, lock : DistVectorWriterLock) {
		instances[0].slots[0] = new DistVectorSlot(eltType);
		instances[1].slots[0] = instances[0].slots[0];
		writeLock = lock;
		pid = _newPrivatizedClass(this);
	}

 	proc DistVectorImpl(other, privData, type eltType = other.eltType) {
		instanceIdx.write(0);
		instances[0].slots[0] = other.instances[0].slots[0];
		instances[1].slots[0] = instances[0].slots[0];
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

    // Expands the array to the next chunk size.
    proc expand(size : int) {
    	acquireWrite();

		// Allocate memory in a block-cyclic manner.
		var instIdx = instanceIdx.read();
		var numNewSlots = (size / DistVectorChunkSize) + min(size % DistVectorChunkSize, 1);
		var newSlots : [{1..0}] DistVectorSlot(eltType);
		var currLocId = 0;
		for i in 1 .. numNewSlots {
			on Locales[currLocId] {
				newSlots.push_back(new DistVectorSlot(eltType));
			}

			currLocId = (currLocId + 1) % numLocales;
		}

		// We append the allocated slot to the unused instance and set it as the current (cross-node update)
		coforall loc in Locales do on loc {
			var _this = getPrivatizedThis;
			var newInstIdx = !(_this.instanceIdx.read());
			ref newInstance = _this.instances[newInstIdx];
			newInstance.slots.push_back(newSlots);
			_this.instanceIdx.write(newInstIdx);
		}

		// Unblock readers, wait for them to finish, then release.
		waitForReaders(instIdx);
		releaseWrite();
    }

    // Indexes into the distributed vector
    proc this(idx : int) ref {
    	var rcIdx = acquireRead();
    	ref instance = currInstance;

    	// If the index is currently allocated, then we simply
    	// fetch the actual element being referenced.
    	if instance.isAllocated(idx) {
    		ref elt = instance[idx];
	    	releaseRead(rcIdx);
	    	return elt;
    	}
    	releaseRead(rcIdx);
    	halt("idx #", idx, " is out of bounds... max=", instance.slots.size * DistVectorChunkSize, ", instIdx=", instanceIdx.read());
    }

    proc contains(elt: eltType): bool {
    	var rcIdx = acquireRead();
    	var instance = currInstance;
    	var found : atomic bool;
    	
    	forall slot in instance.slots {
    		if !found.read() {
    			on slot do for eltIdx in 1 .. DistVectorChunkSize {
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
}

record DistVectorInstance {
	type eltType;
	var slots : [{0..0}] DistVectorSlot(eltType);

	// Determines if the requested index currently exists, which is true if and only if
	// the slot is allocated and that the position in the slot requested is in use.
	proc isAllocated(idx : int) {
		return slots.size > (idx / DistVectorChunkSize);
	}

	proc this(idx : int) ref {
		var slotIdx = idx / DistVectorChunkSize;
		var elemIdx = (idx % DistVectorChunkSize) + 1;
		return slots[slotIdx].elems[elemIdx];
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

