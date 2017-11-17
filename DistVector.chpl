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
		var pid : int;

	 	proc DistVectorImpl(other, privData, type eltType = other.eltType) {
	     
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
	}

}
