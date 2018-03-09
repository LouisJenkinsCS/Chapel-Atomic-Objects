use SysError;
use List;

/*
	UNDER CONSTRUCTION:

	
	Implementation of NORec STM:
		https://anon.cs.rochester.edu/u/scott/papers/2010_ppopp_NOrec.pdf

	TODO List: Add Epoch-Based Garbage Collection where each transaction starts
	in an epoch era and only advances after calling 'commit' or when it calls
	'begin' again. When something is to be deleted, defer deletion until the
	end, which will cause in the succeeding transaction to wait for all conflicting
	transactions to abort... only if they have the conflicting item in their readset


	Example:
		var x = 0;
		var stm = manager.getDescriptor();
		try! {
			stm.beginTransaction();
			stm.write(x, stm.read(x) + 1);
			stm.commitTransaction();
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
type Address = uint(64);
type Value = uint(64);

class STMVersion {
	var version : atomic Version;

	inline proc current {
		return version.read();
	}
}

class LocalSTMManager {
	var spinlock : atomic bool;
	var recycleList : list(STMDescr);
	var clusterVersion : STMVersion;
	var nodeVersion = new STMVersion();

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
		if recycleList.length != 0 {
			descr = recycleList.pop_front();
		}
		release();

		if descr == nil {
			descr = new STMDescr(clusterVersion=clusterVersion, nodeVersion=nodeVersion);
		}

		return descr;
	}

	proc putDescriptor(descr : STMDescr) {
		acquire();
		recycleList.push_front(descr);
		release();
	}
}

/*
	Metadata for STM copies of objects to handle type conversions
	and data integrity checks.
*/
record STMObject {
	var dataSize : uint(64) = 0;
	var copyData : c_ptr(uint(8)) = nil;
	var origData : c_ptr(uint(8)) = nil;

	inline proc toType(type retType) : retType {
		var sz = c_sizeof(retType);
		if sz > dataSize {
			halt("Request of size: ", sz, ", but only have enough data for: ", dataSize);
		}

		return (copyData : c_ptr(retType))[0];
	}

	inline proc allocateFor(ptr : c_ptr(?dataType)) {
		var sz = c_sizeof(dataType);
		if copyData != nil {
			c_free(copyData);
		}

		copyData = c_calloc(uint(8), sz);
		dataSize = sz;

		(copyData : c_ptr(dataType))[0] = ptr[0];
		origData = ptr;
	}

	inline proc validate() : bool {
		return c_memcmp(origData, copyData, dataSize) == 0;
	}

	// Note: Do not invoke object destructor, just delete current shallow copy
	proc ~STMObject() {
		if copyData != nil then c_free(copyData);
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

	proc STMDescr(clusterVersion:STMVersion, nodeVersion:STMVersion) {
		this.clusterVersion = clusterVersion;
		this.nodeVersion = nodeVersion;
	}

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
					throw new STMAbort(STM_CONFLICT);
				}
			}

			// Return the version we have verified for
			if version == nodeVersion.current {
				return version;
			}
		}

		halt("Should not have reached the end of validate...");
	}

	proc beginTransaction() {
		readDom.clear();
		writeDom.clear();
		localVersion = nodeVersion.current;	
		while((localVersion & 1) != 0) {
			chpl_task_yield();
			localVersion = nodeVersion.current;
		}
	}

	proc read(ref val) : Value {
		return read(c_ptrTo(val));
	}

	proc read(val : c_ptr(?dataType)) : dataType {
		//var sz = c_sizeof(dataType);
		//var dataPtr = c_ptrTo(data);
		// Read contents at each address...
		//for addr in 0..#sz by c_sizeof(uint(64)) {
		//	c_memset(c_ptrTo(data), )
		//}
		return read((here.id : uint(64), __primitive("cast", uint(64), val)));
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

	proc commitTransaction() {
		// Already verified that our readset is fine earlier, we're done
		if writeDom.isEmpty() {
			return;
		}

		// Contest for transactional lock
		while !nodeVersion.version.compareExchange(localVersion, localVersion + 1) {
			localVersion = validate();
		}

		// Commit writes to memory
		for _addr in writeDom {
			var addr = __primitive("cast", c_ptr(Value), _addr[2]);
			addr[0] = writeSet[_addr];
		}

		// Release lock, we're done...
		nodeVersion.version.write(localVersion + 2);
	}
}

class O {
	var x = 1;
}

var data : STMObject;
var x : uint(64) = 1;
data.allocateFor(c_ptrTo(x));
writeln("x=", x, ", data=", data.toType(uint(64)));
x = 2;
writeln("x=", x, ", data=", data.toType(uint(64)));
writeln("data.validate=", data.validate());


/*
local {
	var manager = new LocalSTMManager();
	var stm = manager.getDescriptor();

	var o1 : O;
	var o2 : O;
	try! {
		stm.beginTransaction();
		writeln("Reading o1:", stm.read(o1));
		o1 = new object();
		writeln("Invalidated o1...");
		stm.commitTransaction();
		writeln("Successfully committed transaction...");
	} catch retry : STMAbort {
		writeln("Successfully aborted...");
	}
	writeln("Finished");
}
*/