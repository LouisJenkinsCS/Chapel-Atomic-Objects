record RCU {
	type eltType;
	var pid : int = -1;

	proc RCU(type eltType) {
		pid = (new RCUImpl(eltType, new RCUWriteLock())).pid;
	}

	proc _value {
		if pid == -1 {
			halt("RCU is uninitialized...");
		}

		return chpl_getPrivatizedCopy(RCUImpl(eltType), pid);
	}

	forwarding _value;
}

class RCUDescriptorTable {
	type eltType;
	var objs : 2 * eltType;
	var current : atomic int;

	proc RCUDescriptorTable(type eltType) {
		current.write(1);
	}

	proc ref read() : eltType {
		return objs[current.read()];
	}

	proc write(ref obj : eltType) {
		var newCurrent = if current.read() == 1 then 2 else 1;
		objs[newCurrent] = obj;
		current.write(newCurrent);
	}
}

class RCUWriteLock {
	var lock$ : sync bool;
}

class RCUImpl {
	type eltType;
	var pid : int;
	var descriptorTable : RCUDescriptorTable(eltType);
	var readCount : atomic int;
	var writeLock : RCUWriteLock;

	proc RCUImpl(type eltType, writeLock) {
		this.descriptorTable = new RCUDescriptorTable(eltType);
		this.writeLock = writeLock;

		pid = _newPrivatizedClass(this);
	}

	proc RCUImpl(other : RCUImpl, privData, type eltType = other.eltType) {
		this.writeLock = other.writeLock;
		this.descriptorTable = new RCUDescriptorTable(eltType);
		this.pid = other.pid;
	}

	pragma "no doc"
    proc dsiPrivatize(privData) {
        return new RCUImpl(this, privData);
    }

    pragma "no doc"
    proc dsiGetPrivatizeData() {
      return pid;
    }

    pragma "no doc"
    inline proc getPrivatizedThis {
      return chpl_getPrivatizedCopy(this.type, pid);
    }

	proc readBarrier(f) {
		readCount.add(1);

		var val = descriptorTable.objs[descriptorTable.current.read()];
		f(val);

		readCount.sub(1);
	}

	proc writeBarrier(f) {
		writeLock.lock$ = true;

		var val = descriptorTable.objs[descriptorTable.current.read()];
		var newCurrent = if descriptorTable.current.read() == 1 then 2 else 1;
		var newVal = f(val);

		coforall loc in Locales do on loc {
			var localThis = getPrivatizedThis;
			localThis.descriptorTable.objs[newCurrent] = newVal;
			localThis.descriptorTable.current.write(newCurrent);
			localThis.readCount.waitFor(0);
		}

		delete val;

		writeLock.lock$;
	}
}