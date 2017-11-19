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

module DistributedVector {
	record DistVector {
		type eltType;

		// Privatization id
		var pid : int = -1;

		proc DistVector(type eltType) {
			pid = (new DistVectorImpl(eltType)).pid;
		}

		proc ~DistVector() {
			coforall loc in Locales do on loc do delete _value;
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
		var instances : [1..2] [1..64] DistVectorContainer(eltType);
		// Current RCU instance index
		var instanceIdx : atomic int;
		// Current number of readers (TODO: Need Task-Local-Storage)
		var readCount : atomic int;
		// Lock all writers must acquire; it is allocated on a single node to ensure they all
		// may linearize on it.
		var writeLock : DistVectorWriterLock;

		// Acquires reader privilege
		proc getInstance() {
			var readCount.add(1);
			var idx = instanceIdx.read();
			return instances[idx];
		}

		// Requires writer privilege
		proc switchInstance() {
			var idx = instanceIdx.read();
			if idx == 1 then instanceIdx.write(2);
			else instanceIdx.write(1);
		}


		proc DistVectorImpl(type eltType, lock : DistVectorWriterLock) {
			instances[1] = new DistVectorImpl_(eltType);
			instances[2] = new DistVectorImpl_(eltType);
			instanceIdx.write(1);

			writeLock = lock;
			pid = _newPrivatizedClass(this);
		}

	 	proc DistVectorImpl(other, privData, type eltType = other.eltType) {
	    	instances[1] = new DistVectorImpl_(eltType);
			instances[2] = new DistVectorImpl_(eltType);
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

	    inline proc acquireRCU {

	    }
	}

	record DistVectorContainer {
		type eltType;
		var dataDom : {0..-1};
		var data : [dataDom] DistVectorMutableSingleton;

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
	}

}
