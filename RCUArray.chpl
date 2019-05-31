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
config param RCUArrayBlockSize = 1024;

/*
   A resizable, indexable, distributed array.
   */
pragma "always RVF"
record RCUArray {
  type eltType;

  // Instance for locale it was allocated on
  var instance : unmanaged RCUArrayImpl(eltType);
  // Privatization id
  var pid : int = -1;

  proc init(type eltType, initialSize = 0) {
    this.eltType = eltType;
    this.instance = new unmanaged RCUArrayImpl(eltType, initialSize);
    this.pid = this.instance.pid;
  }

  proc _value {
    if pid == -1 {
      halt("RCUArray is uninitialized...");
    }

    return chpl_getPrivatizedCopy(this.instance.type, pid);
  }

  forwarding _value;
}

class RCUArrayImpl {
  type eltType;

  // Privatization id
  var pid : int;

  // Our current snapshot
  var snapshot : unmanaged RCUArraySnapshot(eltType);

  // Denotes the current version of the snapshot. This is a privatized field
  var globalEpoch : chpl__processorAtomicType(int);

  // The current read count for a snapshot. As we can only have a single writer
  // at any given time, we can only have two snapshots alive (the old one, and the
  // new one the writer just created), and so we can alternate between both counters.
  var epochReaders : [0..1] chpl__processorAtomicType(int);

  // Lock that the solitary writer must acquire. To ensure that we can manage the data
  // structure across the cluster, we have one coarse-grained lock for writers that must
  // be allocated on a single node.
  var writeLock : unmanaged RCUArrayWriterLock;

  // The next locale to allocate the next chunk of data on.
  var nextLocaleAlloc = 1;

  // Enter read-side critical section. Returns the current epoch version acquired
  // which is needed to appropriately exit the read-side critical section.
  proc rcu_read_lock() : int {
    var epoch : int;

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

    return epoch;
  }

  // Releases read-access to the current instance. The index must be the same
  // that is obtained from the 'acquireRead' read-barrier 
  proc rcu_read_unlock(idx : int) {
    epochReaders[idx].sub(1);
  }

  proc init(type eltType, initialSize : integral) {
    this.eltType = eltType;
    this.snapshot = new unmanaged RCUArraySnapshot(eltType);
    this.complete();
    var initialChunks = alloc(initialSize, 0, numLocales);
    this.snapshot.chunks.push_back(initialChunks);
    this.writeLock = new unmanaged RCUArrayWriterLock();
    this.pid = _newPrivatizedClass(this);
  }

  proc init(other, privData, type eltType = other.eltType) {
    this.eltType = eltType;
    this.pid = other.pid;
    this.snapshot = new unmanaged RCUArraySnapshot(eltType);
    this.snapshot.chunks.push_back(other.snapshot.chunks);
    this.writeLock = other.writeLock;
  }

  proc deinit() {
    delete snapshot;
  }

  pragma "no doc"
    proc dsiPrivatize(privData) {
      return new unmanaged RCUArrayImpl(this, privData);
    }

  pragma "no doc"
    proc dsiGetPrivatizeData() {
      return pid;
    }

  pragma "no doc"
    inline proc getPrivatizedInstance() {
      return chpl_getPrivatizedCopy(this.type, this.pid);
    }

  proc alloc(size, startLocId, nLocales) {
    // If size is '0' do not expand, otherwise allocate at least one...
    var numChunks = if size == 0 then 0 else (size / RCUArrayBlockSize) + min(size % RCUArrayBlockSize, 1);
    var newChunks : [{1..numChunks}] _ddata(eltType);
    var currLocId = startLocId % nLocales;
    for i in 1 .. numChunks {
      on Locales[currLocId] {
        newChunks[i] = _ddata_allocate(eltType, RCUArrayBlockSize);
      }

      currLocId = (currLocId + 1) % nLocales;
    }

    return newChunks;
  }

  // Expands the array to the next nearest chunk size.
  // Returns the new size as an interval (start, finish)
  proc expand(size : int) : (int, int) {
    writeLock.lock();

    // Allocate memory in a block-cyclic manner.
    var newSlots = alloc(size, nextLocaleAlloc, numLocales);
    nextLocaleAlloc += newSlots.size;

    // We append the allocated slot to the unused instance 
    // and set it as the current (cross-node update)
    coforall loc in Locales do on loc {
      var _this = getPrivatizedInstance();
      var currentEpoch = _this.globalEpoch.read();
      var oldEpoch = currentEpoch % 2;
      var newEpoch = !oldEpoch;
      var oldSnapshot = _this.snapshot;
      var newSnapshot = new unmanaged RCUArraySnapshot(eltType);

      // Move chunks in older instance into the newer and append
      // the newer slots.
      newSnapshot.chunks.push_back(oldSnapshot.chunks);
      newSnapshot.chunks.push_back(newSlots);

      // Update epochs and wait for readers using old epoch
      _this.snapshot = newSnapshot;
      _this.globalEpoch.write(currentEpoch + 1);
      while(_this.epochReaders[oldEpoch].read() > 0) {
        chpl_task_yield();
      }

      // At this point, no other task will be using this, so delete it
      delete oldSnapshot;
    }

    var retval = (0, (snapshot.chunks.size * RCUArrayBlockSize) - 1);
    writeLock.unlock();
    return retval;
  }

  // Indexes into the distributed vector
  proc this(idx : int) ref {
    var rcIdx = rcu_read_lock();

    // If the index is currently allocated, then we simply
    // fetch the actual element being referenced.
    if !boundsChecking || snapshot.isAllocated(idx) {
      ref elt = snapshot[idx];
      rcu_read_unlock(rcIdx);
      return elt;
    }
    rcu_read_unlock(rcIdx);

    halt("idx #", idx, " is out of bounds... max=", snapshot.chunks.size * RCUArrayBlockSize, 
        ", globalEpoch=", globalEpoch.read());
  }

  proc size : int {
    var rcIdx = rcu_read_lock();
    var sz = snapshot.chunks.size * RCUArrayBlockSize;
    rcu_read_unlock(rcIdx);
    return sz;
  }
}


class RCUArrayChunk {
  type eltType;
  var elems : c_array(eltType, RCUArrayBlockSize);
}

class RCUArraySnapshot {
  type eltType;

  // Each chunk is allocated on a one of the nodes we are distributed over.
  // TODO: Find a way to keep track of wide_ptr_t to directly return by reference
  // TODO: Use c_void_ptr, make the distribution deterministic in terms of finding the
  // owning node, then cast the offset into the chunk into 'eltType' and return that as a ref.
  pragma "no copy"
  pragma "local field"
  var chunks : [0..-1] _ddata(eltType);

  // Determines if the requested index currently exists, which is true if and only if
  // the slot is allocated and that the position in the slot requested is in use.
  proc isAllocated(idx : int) {
    return chunks.size > (idx / RCUArrayBlockSize);
  }

  proc this(idx : int) ref {
    var chunkIdx = idx / RCUArrayBlockSize;
    var elemIdx = idx % RCUArrayBlockSize;
    pragma "no copy" pragma "no auto destroy" var chunk = chunks[chunkIdx];
    return chunk[elemIdx];
  }
}

class RCUArrayWriterLock {
  var lock$ : sync bool;

  inline proc lock() {
    lock$ = true;
  }

  inline proc unlock() {
    lock$;
  }
}

