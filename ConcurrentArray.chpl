use SharedObject;

/*
	A concurrent array implementation that allows concurrent access while resizing.
	The concurrent array makes use of the epoch-based reclamation strategy where
	readers must enter what is referred to as 'read-side' critical sections. These
	critical sections are not to provide mutual exclusion, as unlike reader-writer
	locks we allow writes to occur concurrently, but to ensure that writers do not 
	reclaim memory while it is in use.
*/

/*
	Size of each contiguous chunk.
*/ 
config param ConcurrentArrayChunkSize = 1024;

/*
	Whether or not we perform read barriers. This can be set
	to false for niche performance testing where you know there
	will be no concurrent writer.
*/
config param ConcurrentArrayUseQSBR = false;

/*
	Reference Counting Mechanism
*/
pragma "no doc"
class ConcurrentArrayRC {
	type eltType;
	var _pid : int;

    proc deinit() {
    	var _value = chpl_getPrivatizedCopy(ConcurrentArrayImpl(eltType), _pid);
    	// Chunks are shared across nodes, so we must take care to
    	// delete them only once.
    	forall chunk in _value.snapshot.chunks do delete chunk;

    	// Delete the per-node data
		coforall loc in Locales do on loc do {
			delete chpl_getPrivatizedCopy(ConcurrentArrayImpl(eltType), _pid);
		}
   	}
}

/*
	A resizable, indexable, distributed array.
*/
record ConcurrentArray {
	type eltType;

	// Privatization id
	var pid : int = -1;

	// Reference Counting...
    pragma "no doc"
    var _rc : Shared(ConcurrentArrayRC(eltType));

	proc ConcurrentArray(type eltType, initialCap = 0) {
		pid = (new ConcurrentArrayImpl(eltType, new ConcurrentArrayWriterLock(), initialCap)).pid;
	}

	proc _value {
		if pid == -1 {
			halt("ConcurrentArray is uninitialized...");
		}

		return chpl_getPrivatizedCopy(ConcurrentArrayImpl(eltType), pid);
	}

	forwarding _value;
}

class ConcurrentArrayImpl {
	type eltType;

	// Privatization id
	var pid : int;

	// Our current snapshot
	var snapshot : ConcurrentArraySnapshot(eltType);

	// Denotes the current version of the snapshot. This is a privatized field
	var globalEpoch : atomic int;

	// The current read count for a snapshot. As we can only have a single writer
	// at any given time, we can only have two snapshots alive (the old one, and the
	// new one the writer just created), and so we can alternate between both counters.
	var epochReaders : [0..1] atomic int;

	// Lock that the solitary writer must acquire. To ensure that we can manage the data
	// structure across the cluster, we have one coarse-grained lock for writers that must
	// be allocated on a single node.
	var writeLock : ConcurrentArrayWriterLock;

	// The next locale to allocate the next chunk of data on.
	var nextLocaleAlloc = 1;

	// Enter read-side critical section. Returns the current epoch version acquired
	// which is needed to appropriately exit the read-side critical section.
	proc rcu_read_lock() : int {
		var epoch : int;

		if !ConcurrentArrayUseQSBR {
			

			// It is possible for a writer to change the current epoch between 
			// our read and increment, so we must loop until we succeed. Note that
			// this makes livelock possible, but is extremely rare and writers are
			// infrequent compared to readers.
			do {
				var currentEpoch = globalEpoch.read();
				epoch = currentEpoch % 2;
				epochReaders[epoch].add(1);

				// Writers will change the global epoch prior to waiting on readers. 
				if currentEpoch != globalEpoch.read() {
					// Undo reader count, loop again.
					epochReaders[epoch].sub(1);
					continue;
				}
			} while (false);
		}
		
		return epoch;
	}

	// Releases read-access to the current instance. The index must be the same
	// that is obtained from the 'acquireRead' read-barrier 
	proc rcu_read_unlock(idx : int) {
		if !ConcurrentArrayUseQSBR {
			epochReaders[idx].sub(1);
		}
	}

	proc ConcurrentArrayImpl(type eltType, lock : ConcurrentArrayWriterLock, initialCap) {
		this.snapshot = new ConcurrentArraySnapshot(eltType);
		var initialChunks = alloc(initialCap, 0, numLocales);
		this.snapshot.chunks.push_back(initialChunks);
		this.writeLock = lock;
		this.pid = _newPrivatizedClass(this);
	}

 	proc ConcurrentArrayImpl(other, privData, type eltType = other.eltType) {
		this.snapshot = new ConcurrentArraySnapshot(eltType);
		this.snapshot.chunks.push_back(other.snapshot.chunks);
		this.writeLock = other.writeLock;
		this.pid = other.pid;
    }

    proc ~ConcurrentArrayImpl() {
    	delete snapshot;
    }

    pragma "no doc"
    proc dsiPrivatize(privData) {
        return new ConcurrentArrayImpl(this, privData);
    }

    pragma "no doc"
    proc dsiGetPrivatizeData() {
      return pid;
    }

    pragma "no doc"
    inline proc getPrivatizedThis {
      return chpl_getPrivatizedCopy(this.type, this.pid);
    }

    proc alloc(size, startLocId, nLocales) {
    	var numChunks = (size / ConcurrentArrayChunkSize) + min(size % ConcurrentArrayChunkSize, 1);
		var newChunks : [{1..numChunks}] ConcurrentArrayChunk(eltType);
		var currLocId = startLocId % nLocales;
		for i in 1 .. numChunks {
			on Locales[currLocId] {
				newChunks[i] = new ConcurrentArrayChunk(eltType);
			}

			currLocId = (currLocId + 1) % nLocales;
		}

		return newChunks;
    }

    // Expands the array to the next nearest chunk size.
    proc expand(size : int) {
    	writeLock.lock();

		// Allocate memory in a block-cyclic manner.
		var newSlots = alloc(size, nextLocaleAlloc, numLocales);
		nextLocaleAlloc += newSlots.size;

		// We append the allocated slot to the unused instance 
		// and set it as the current (cross-node update)
		coforall loc in Locales do on loc {
			var _this = getPrivatizedThis;
			var currentEpoch = _this.globalEpoch.read();
			var oldEpoch = currentEpoch % 2;
			var newEpoch = !oldEpoch;
			var oldSnapshot = _this.snapshot;
			var newSnapshot = new ConcurrentArraySnapshot(eltType);

			// Move chunks in older instance into the newer and append
			// the newer slots.
			newSnapshot.chunks.push_back(oldSnapshot.chunks);
			newSnapshot.chunks.push_back(newSlots);
			
			// Update epochs and wait for readers using old epoch
			_this.snapshot = newSnapshot;
			if ConcurrentArrayUseQSBR {
				extern proc chpl_qsbr_defer_deletion(c_void_ptr, int, bool);
				var storage = c_malloc(c_void_ptr, 1);
				storage[0] = oldSnapshot : c_void_ptr;
				chpl_qsbr_defer_deletion(storage : c_void_ptr, 1, true);
			} else {
				_this.globalEpoch.write(currentEpoch + 1);
				while(_this.epochReaders[oldEpoch].read() > 0) {
					chpl_task_yield();
				}

				// At this point, no other task will be using this, so delete it
				delete oldSnapshot;
			}
		}

		writeLock.unlock();
    }

    // Indexes into the distributed vector
    proc this(idx : int) ref {
    	var rcIdx = rcu_read_lock();
    	
    	// If the index is currently allocated, then we simply
    	// fetch the actual element being referenced.
    	if snapshot.isAllocated(idx) {
    		ref elt = snapshot[idx];
	    	rcu_read_unlock(rcIdx);
	    	return elt;
    	}
    	rcu_read_unlock(rcIdx);
    	halt("idx #", idx, " is out of bounds... max=", snapshot.chunks.size * ConcurrentArrayChunkSize, ", globalEpoch=", globalEpoch.read());
    }

    proc contains(elt: eltType): bool {
    	var rcIdx = rcu_read_lock();
    	var found : atomic bool;
    	
    	forall slot in snapshot.chunks {
    		if !found.read() {
    			on slot do for eltIdx in 1 .. ConcurrentArrayChunkSize {
    				if elt == slot[eltIdx] {
    					found.write(true);
    					break;
    				}
    			}
    		}
    	}

    	rcu_read_unlock(rcIdx);
    	return found.read();
    }

    proc size : int {
    	var rcIdx = rcu_read_lock();
    	var sz = snapshot.chunks.size * ConcurrentArrayChunkSize;
    	rcu_read_unlock(rcIdx);
    	return sz;
    }
}


class ConcurrentArrayChunk {
	type eltType;
	var elems : (ConcurrentArrayChunkSize * eltType);
}

class ConcurrentArraySnapshot {
	type eltType;

	// Each chunk is allocated on a one of the nodes we are distributed over.
	// TODO: Find a way to keep track of wide_ptr_t to directly return by reference
	var chunks : [0..-1] ConcurrentArrayChunk(eltType);

	// Determines if the requested index currently exists, which is true if and only if
	// the slot is allocated and that the position in the slot requested is in use.
	proc isAllocated(idx : int) {
		return chunks.size > (idx / ConcurrentArrayChunkSize);
	}

	proc this(idx : int) ref {
		var chunkIdx = idx / ConcurrentArrayChunkSize;
		var elemIdx = (idx % ConcurrentArrayChunkSize) + 1;
		return chunks[chunkIdx].elems[elemIdx];
	}
}

class ConcurrentArrayWriterLock {
	var lock$ : sync bool;

	inline proc lock() {
		lock$ = true;
	}

	inline proc unlock() {
		lock$;
	}
}

