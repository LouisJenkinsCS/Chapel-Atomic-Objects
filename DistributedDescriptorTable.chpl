use ConcurrentArray;
use SharedObject;
use LocalAtomicObject;

/*
	Reference Counting Mechanism
*/
pragma "no doc"
class DistributedDescriptorTableReferenceCount {
	type objType;
	var _pid : int;

    proc deinit() {
    	// Delete the per-node data
		coforall loc in Locales do on loc do {
			var instance = chpl_getPrivatizedCopy(DistributedDescriptorTableImpl(objType), _pid);
			//delete instance.recycledDescriptors;
			//delete instance;
		}
   	}
}

/*
	A resizable, indexable, distributed array.
*/
record DistributedDescriptorTable {
	type objType;

	// Privatization id
	var pid : int = -1;

	// Reference Counting...
    pragma "no doc"
    var _rc : Shared(DistributedDescriptorTableReferenceCount(objType));

	proc DistributedDescriptorTable(type objType, initialCap = 0) {
		pid = (new DistributedDescriptorTableImpl(objType, initialCap)).pid;
		_rc = new Shared(new DistributedDescriptorTableReferenceCount(objType, _pid=pid));
	}

	proc _value {
		if pid == -1 {
			halt("DistributedDescriptorTable is uninitialized...");
		}

		return chpl_getPrivatizedCopy(DistributedDescriptorTableImpl(objType), pid);
	}

	forwarding _value;
}

class StackNode {
	type eltType;
	var elt : eltType;
	var next : StackNode(eltType);
}

class DistributedDescriptorTableImpl {
	type objType;
	var pid : int = -1;

	// TODO: Investigate default constructor work performed...
	var descriptorTable : ConcurrentArray(objType);
	var recycledDescriptors : LocalAtomicObject(StackNode(int));

	proc DistributedDescriptorTableImpl(type objType) {
		this.pid = _newPrivatizedClass(this);
	}

	proc DistributedDescriptorTableImpl(other, privData, type objType = other.objType) {
		this.descriptorTable = other.descriptorTable;
		this.pid = other.pid;
	}

	proc ~DistributedDescriptorTableImpl() {
		//delete recycledDescriptors;
	}

    proc dsiPrivatize(privData) {
        return new DistributedDescriptorTableImpl(this, privData);
    }

    proc dsiGetPrivatizeData() {
      return pid;
    }

    inline proc getPrivatizedThis {
      return chpl_getPrivatizedClass(this.type, this.pid);
    }

    proc getRecycledDescriptor() : (bool, int) {
    	while true {
	    	var head = recycledDescriptors.read();
	    	if (head == nil) {
	    		return (false, 0);
	    	}
			if (recycledDescriptors.compareExchange(head, head.next)) {
				// TODO: Use QSBR for memory reclamation once accepted...
				return (true, head.elt);
			}
    	}

    	return (false, 0);
    }

    // Inserts the passed object into an empty space in the table; if one is not found
    // the table will be reallocated. This is concurrent safe, but concurrent accesses
    // while the table is out of space may result in repeated resize operations.
    proc register(obj : objType) : int {
    	// Find a descriptor
    	var (hasDescr, descr) = getRecycledDescriptor();

    	if (!hasDescr) {
    		// If we cannot recycle any descriptors, grow the table...
	    	var (start, end) = descriptorTable.expand(ConcurrentArrayChunkSize); 
	    	
	    	// Recycle rest of descriptors, keep one for self...
    	
	    	var head : StackNode(int);
	    	var tail : StackNode(int);
	    	for descr in (end - ConcurrentArrayChunkSize) .. end - 1 {
    			var node = new StackNode(int, elt=descr);
    			if head == nil {
    				head = node;
    				tail = head;
    			} else {
    				node.next = head;
    				head = node;
    			}
	    	}

	    	while true {
	    		var oldHead = recycledDescriptors.read();
	    		tail.next = oldHead;
	    		if recycledDescriptors.compareExchange(oldHead, head) {
	    			break;
	    		}
	    	}

	    	// Claim last descriptor
	    	descr = end;
    	}

    	// Register in table...
    	descriptorTable[descr] = obj;

    	return descr;
    }

    proc unregister(descr : int) {
    	var node = new StackNode(int, elt=descr);
    	while true {
    		var oldHead = recycledDescriptors.read();
    		node.next = oldHead;
    		if recycledDescriptors.compareExchange(oldHead, node) {
    			break;
    		}
    	}
    }

    proc this(idx : int) ref {
    	return descriptorTable[idx];
    }

    proc size {
    	return descriptorTable.size;
    }
}


proc main() {
	class O {
		var descr : int;
	}
	var nIterations = 1024 * 1024;
	var dt : DistributedDescriptorTable(O) = new DistributedDescriptorTable(O);

	coforall loc in Locales do on loc {
		forall 1 .. nIterations {
			var o = new O();
			o.descr = dt.register(o);
		}
	}

	forall ix in 0 .. #dt.size {
		assert(dt[ix].descr == ix);
	}

	writeln("Success!");
}