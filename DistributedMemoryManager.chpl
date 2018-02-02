use SharedObject;

class DistributedMemoryManagerReferenceCounter {
	var pid : int;

    proc deinit() {
    	var _value = chpl_getPrivatizedCopy(DistributedMemoryManagerImpl, pid);

    	// Delete all epochs
    	delete _value.master;
    	for arr in _value.slave do delete _value.slave;

    	// Delete the per-node data
		coforall loc in Locales do on loc do {
			delete chpl_getPrivatizedCopy(DistributedMemoryManagerImpl, pid);
		}
   	}
}

record DistributedMemoryManager {
	var pid : int = -1;
	var refCount : Shared(DistributedMemoryManagerReferenceCounter);

	proc DistributedMemoryManager() {
		pid = (new DistributedMemoryManagerImpl()).pid;
		refCount = new Shared(new DistributedMemoryManagerReferenceCounter(pid=pid));
	}

	proc _value {
		if pid == -1 {
			halt("DistributedMemoryManager is uninitialized...");
		}

		return chpl_getPrivatizedCopy(DistributedMemoryManagerImpl, pid);
	}

	forwarding _value;
}

class DeferNode {
	var epoch : uint(64);
	var obj : object;
	var next : DeferNode;
}

class DistributedMemoryManagerImpl {
	var master : DistributedMemoryManagerEpoch;
	var slave : [LocaleSpace] DistributedMemoryManagerEpoch;
	var pid : int;
	var list : DeferNode;
	var listLock$ : sync bool;

	proc DistributedMemoryManagerImpl() {
		master = new DistributedMemoryManagerEpoch();
		for i in 0 .. #numLocales {
			on Locales[i] do slave[i] = new DistributedMemoryManagerEpoch();
		}

		pid = _newPrivatizedClass(this);
	}

	proc DistributedMemoryManagerImpl(other, privData) {
		this.master = other.master;
		this.slave = other.slave;
		this.pid = other.pid;
	}

	proc ~DistributedMemoryManagerImpl() {
		while (list) {
			var tmp = list;
			list = list.next;
			delete tmp.obj;
			delete tmp;
		}
	}

    proc dsiPrivatize(privData) {
        return new DistributedMemoryManagerImpl(this, privData);
    }

    proc dsiGetPrivatizeData() {
      return pid;
    }

    inline proc getPrivatizedThis {
      return chpl_getPrivatizedCopy(this.type, this.pid);
    }

    inline proc localSlave ref {
    	return slave[here.id];
    }

    inline proc currentEpoch {
    	return localSlave.epoch.read();
    }

    inline proc getMinimumEpoch() {
    	var epochs : [LocaleSpace] uint(64);
    	forall i in 0 .. #numLocales {
    		epochs[i] = slave[i].epoch.read();
    	}

    	var minEpoch : uint(64) = (-1) : uint(64);
    	for e in epochs {
    		minEpoch = if e < minEpoch then e else minEpoch;
    	}
    	return minEpoch;
    }

    proc checkpoint() {
    	// Observe new epoch
    	localSlave.epoch.write(master.epoch.read());
    	
    	// Find smallest and safest epoch to reclaim
    	var minEpoch = getMinimumEpoch();
    	deleteBefore(minEpoch);
    }

    proc reclaim(o : object) {
    	listLock$ = true;
    	var dnode = new DeferNode(obj = o, epoch = currentEpoch, next = list);
    	list = dnode;
    	listLock$;
    }

    proc publish() {
    	localSlave.epoch.write(master.epoch.fetchAdd(1) + 1);
    }

    proc deleteBefore(epoch) {
    	var head : DeferNode;
    	listLock$ = true;
    	if (list != nil) {
    		if (epoch > list.epoch) {
    			head = list;
    			list = nil;
    		} else {
    			var prev = list;
    			var curr = list.next;

    			while (curr != nil && curr.epoch >= epoch) {
    				prev = curr;
    				curr = curr.next;
    			}

    			prev.next = nil;
    			head = curr;
    		}
    	}
    	listLock$;

    	while (head != nil) {
    		var tmp = head;
    		head = head.next;
    		delete tmp.obj;
    		delete tmp;
    	}
    }
}

class DistributedMemoryManagerEpoch {
	var epoch : atomic uint(64);
}

proc main() {
	var dmm : DistributedMemoryManager;
	var arr : [0..(numLocales * 1024)] object;

	coforall loc in Locales do on loc {
		forall i in here.id * 1024  .. ((here.id + 1) * 1024) - 1 {
			arr[i] = new object();
		} 
	}

	var locid = 0;
	forall a in arr {
		on Locales[locid % numLocales] {
			dmm.reclaim(a);
			dmm.publish();
		}
	}

	coforall loc in Locales do on loc do dmm.checkpoint();
	writeln("Checkpoint, List: ", dmm.list : c_void_ptr : uint(64));
}