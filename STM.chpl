use SysError;

/*
	Implementation of NORec STM:
		https://anon.cs.rochester.edu/u/scott/papers/2010_ppopp_NOrec.pdf
*/

pragma "use default init"
class STMAbort : Error { }

pragma "use default init"
class STMRetry : Error { }

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

class STMDescr {
	// ReadSet
	var readDom : domain(Address);
	var readSet : [readDom] Value;

	// WriteSet
	var writeDom : domain(Address);
	var writeSet : [writeDom] Value;

	// Version of transaction
	var globalVersion : STMVersion;
	var localVersion : Version;

	proc STMDescr(ver : STMVersion) {
		globalVersion = ver;
	}

	inline proc appendRead(addr : Address, val : Value) {
		readDom += addr;
		readSet[addr] = val;
	}

	inline proc appendWrite(addr : Address, val : Value) {
		writeDom += addr;
		writeSet[addr] = val;
	}

	inline proc validate() : Version throws {
		while true {
			var version = globalVersion.current;
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
					throw new STMRetry();
				}
			}

			// Return the version we have verified for
			if version == globalVersion.current {
				return version;
			}
		}

		halt("Should not have reached the end of validate...");
	}

	proc STMBegin() {
		localVersion = globalVersion.current;	
		while((localVersion & 1) != 0) {
			chpl_task_yield();
			localVersion = globalVersion.current;
		}
	}

	proc STMRead(addr : Address) : Value {
		// If writeSet contains the address, return it
		if writeDom.member(addr) {
			return writeSet[addr];
		} else if readDom.member(addr) {
			return readSet[addr];
		}

		var ptr = __primitive("cast", c_ptr(Value), addr[2]);
		var val = ptr[0];
		
		// Validate again if another transaction was completed recently
		while (localVersion != globalVersion.current) {
			localVersion = validate();
			val = ptr[0];
		}

		appendRead(addr, val);
		return val;
	}

	proc STMWrite(addr : Address, val : Value) {
		appendWrite(addr, val);
	}

	proc STMCommit() {
		// Already verified that our readset is fine earlier, we're done
		if writeDom.isEmpty() {
			return;
		}

		// Contest for transactional lock
		while !globalVersion.version.compareExchange(localVersion, localVersion + 1) {
			localVersion = validate();
		}

		// Commit writes to memory
		for _addr in writeDom {
			var addr = __primitive("cast", c_ptr(Value), _addr[2]);
			addr[0] = writeSet[_addr];
		}

		// Release lock, we're done...
		globalVersion.version.write(localVersion + 2);
	}
}

local {
	var o1 : object;
	var o2 : object;
	var stm = new STMDescr(new STMVersion());
	stm.STMBegin();
	try! {
		writeln("Reading o1:", stm.STMRead(toAddress(c_ptrTo(o1))));
		o1 = new object();
		writeln("Invalidated o1...");
		stm.validate();
	} catch retry : STMRetry {
		writeln("Successfully aborted...");
	}
	writeln("Finished");
}