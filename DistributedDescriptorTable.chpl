use ConcurrentArray;
use SharedObject;
use DistributedBag;

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
		writeln("Table pid: ", pid);
		_rc = new Shared(new DistributedDescriptorTableReferenceCount(objType, _pid=pid));
	}

	proc _value {
		writeln("Retrieved pid: ", pid);
		if pid == -1 {
			halt("DistributedDescriptorTable is uninitialized...");
		}

		return chpl_getPrivatizedCopy(DistributedDescriptorTableImpl(objType), pid);
	}

	forwarding _value;
}

class DistributedDescriptorTableImpl {
	type objType;
	var pid : int = -1;

	// TODO: Investigate default constructor work performed...
	var descriptorTable : ConcurrentArray(objType);
	var recycledDescriptors : DistBag(int);

	proc DistributedDescriptorTableImpl(type objType) {
		this.recycledDescriptors = new DistBag(int, targetLocales = Locales[here.id .. here.id]);
		this.pid = _newPrivatizedClass(this);
	}

	proc DistributedDescriptorTableImpl(other, privData, type objType = other.objType) {
		this.recycledDescriptors = new DistBag(int, targetLocales = Locales[here.id .. here.id]);
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

    // Inserts the passed object into an empty space in the table; if one is not found
    // the table will be reallocated. This is concurrent safe, but concurrent accesses
    // while the table is out of space may result in repeated resize operations.
    proc register(obj : objType) : int {
    	// Find a descriptor
    	var (hasDescr, descr) = recycledDescriptors.bag.remove();

    	if (!hasDescr) {
    		// If we cannot recycle any descriptors, grow the table...
	    	var (start, end) = descriptorTable.expand(ConcurrentArrayChunkSize); 
	    	
	    	// Recycle rest of descriptors, keep one for self...
	    	// TODO add a real bulk add for this
	    	recycledDescriptors.bag.add((end - ConcurrentArrayChunkSize) .. end - 1);	
	    	descr = end;
    	}

    	// Register in table...
    	descriptorTable[descr] = obj;

    	return descr;
    }

    proc unregister(descr : int) {
    	recycledDescriptors.add(descr);
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
	var x = 0;
	var nIterations = 1024 * 1024;
	var dt : DistributedDescriptorTable(O) = new DistributedDescriptorTable(O);
	writeln("Done initializing...");


	writeln(dt.recycledDescriptors._pid);
	writeln(dt.pid);

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