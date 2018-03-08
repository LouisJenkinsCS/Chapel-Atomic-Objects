use SysError;
use List;

/*
	UNDER CONSTRUCTION:

	
	Implementation of NORec STM:
		https://anon.cs.rochester.edu/u/scott/papers/2010_ppopp_NOrec.pdf


	Example:
		var x = 0;
		var stm = manager.getDescriptor();
		try! {
			stm.begin();
			stm.write(x, stm.read(x) + 1);
			stm.commit();
		} catch retry : STMRetry {
			// ...
		}
*/

extern {
	inline static void *byRef(void *x) { return x; }
}

param STM_CONFLICT = 1;
param STM_ABORT = 2;

pragma "use default init"
class STMAbort : Error { 
	var errCode : int;
}

type Version = uint(64);
type Address = (uint(64), uint(64));
type Value = uint(64);

class STMVersion {
	var version : atomic Version;

	inline proc current {
		return version.read();
	}
}

inline proc toAddress(val) : Address {
	return (here.id, __primitive("cast", uint(64), val));	
}


class LocalSTMManager {
	var spinlock : atomic bool;
	var recycleList : list(STMDescr);
	var clusterVersion : STMVersion;
	var nodeVersion : Version;

	inline proc acquire() {
		if spinlock.testAndSet() {
			while true {
				spinlock.waitFor(false);
				if !spinlock.testAndSet() then break;
			}
		}
	}

	inline proc release() {
		spinlock.clear();
	}

	proc getDescriptor() : STMDescr {
		var descr : STMDescr;
		
		acquire();
		if list.length != 0 {
			descr = recycleList.pop_front();
		}
		release();

		if descr == nil {
			descr = new STMDescr(clusterVersion=clusterVersion, nodeVersion=nodeVersion);
		}
	}

	proc putDescriptor(descr : STMDescr) {
		acquire();
		recycleList.push_front(descr);
		release();
	}
}

/*
	Descriptor that contains metadata for performing transactions.
*/
class STMDescr {
	// ReadSet
	var readDom : domain(Address);
	var readSet : [readDom] Value;

	// WriteSet
	var writeDom : domain(Address);
	var writeSet : [writeDom] Value;

	// Version of transaction
	var clusterVersion : STMVersion;
	var nodeVersion : STMVersion;
	var localVersion : Version;

	inline proc appendRead(addr : Address, val : Value) {
		readDom += addr;
		readSet[addr] = val;
	}

	inline proc appendWrite(addr : Address, val : Value) {
		writeDom += addr;
		writeSet[addr] = val;
	}

	inline proc isReadOnly() : bool {
		return readSet.isEmpty();
	}

	inline proc validate() : Version throws {
		while true {
			var version = nodeVersion.current;
			if (version & 1) != 0 {
				// TODO: Handle when QSBR is added...
				chpl_task_yield();
				continue;
			}

			// TODO: If we are performing this on potentially wide data, we need to 
			// use a PUT/GET operation...
			for _addr in readDom {
				var addr = __primitive("cast", c_ptr(Value), _addr[2]);
				var val = readSet[_addr];

				if (addr[0] != val) {
					throw new STMRetry(STM_CONFLICT);
				}
			}

			// Return the version we have verified for
			if version == nodeVersion.current {
				return version;
			}
		}

		halt("Should not have reached the end of validate...");
	}

	proc begin() {
		readSet.clear();
		writeSet.clear();
		localVersion = nodeVersion.current;	
		while((localVersion & 1) != 0) {
			chpl_task_yield();
			localVersion = nodeVersion.current;
		}
	}

	// Note: Must be multiple of 8
	// Note: Must be a pointer to memory *local* to this node
	proc read(ptr : c_ptr(?dataType)) : dataType {
		var ret : dataType;
		var sz = c_sizeof(dataType);

		// If we already have this data, 
		for 
	}

	// TODO: add a way to handle multi-word data
	proc read(ref obj : dataType) : dataType {
		return STMRead(byRef(obj) : c_ptr(dataType));
		// return STMRead((here.id, __primitive("cast", uint(64), val)));
	}

	proc read(addr : Address) : Value {
		// If writeSet contains the address, return it
		if writeDom.member(addr) {
			return writeSet[addr];
		} else if readDom.member(addr) {
			return readSet[addr];
		}

		var ptr = __primitive("cast", c_ptr(Value), addr[2]);
		var val = ptr[0];
		
		// Validate again if another transaction was completed recently
		while (localVersion != nodeVersion.current) {
			localVersion = validate();
			val = ptr[0];
		}

		appendRead(addr, val);
		return val;
	}

	proc write(addr : Address, val : Value) {
		appendWrite(addr, val);
	}

	proc commit() {
		// Already verified that our readset is fine earlier, we're done
		if writeDom.isEmpty() {
			return;
		}

		// Contest for transactional lock
		while !clusterVersion.version.compareExchange(localVersion, localVersion + 1) {
			localVersion = validate();
		}

		// TODO: Do forall nodes
		nodeVersion.write(localVersion + 1);

		// Commit writes to memory
		for _addr in writeDom {
			var addr = __primitive("cast", c_ptr(Value), _addr[2]);
			addr[0] = writeSet[_addr];
		}

		// Release lock, we're done...
		// TODO: Do forall nodes
		nodeVersion.version.write(localVersion + 2);
		clusterVersion.version.write(localVersion + 2);
	}
}

local {
	var manager = new LocalSTMManager();
	var stm = manager.getDescriptor();

	var o1 : object;
	var o2 : object;
	try! {
		stm.begin();
		writeln("Reading o1:", stm.read(c_ptrTo(o1)));
		o1 = new object();
		writeln("Invalidated o1...");
		stm.validate();
	} catch retry : STMRetry {
		writeln("Successfully aborted...");
	}
	writeln("Finished");
}