# Chapel-Atomic-Objects

The `LocalAtomicObject` is a very simple abstraction that is optimized to ignore the `locale` portion of
a wide pointer (and act directly on the `addr`), and perform normal 64-bit atomic operations that way.
It is a simple yet efficient solution. It used as such:

```chpl
var atomicObj : LocalAtomicObject(Obj);
var a = new Obj();
var b = new Obj();
atomicObj.write(a);
atomicObj.compareAndSwap(a, b);
assert(atomicObject.read() == b);
```

The `GlobalAtomicObject` is an actual novel solution to a very big problem in distributed computing.
Atomic operations on remote memory is a very tricky topic. There are multiple approaches, but as HPC demands
high performance above all else, the number of valid choices dwindle to next to nothing. While specialized
hardware may be developed in the future, I sought to develop a software solution that works in the here-and-now.
However, to understand the actual problem, some background knowledge is required. First, it is used as such:

```chpl
var atomicObj : GlobalAtomicObject(Obj);
var a = new Obj();
var b = new Obj();
atomicObj.write(a);
atomicObj.compareAndSwap(a, b);
assert(atomicObject.read() == b);
```

## Remote Atomic Operations

As a PGAS (Partitioned Global Address Space) language, Chapel allows operations on memory to be transparent 
with respect to which node the memory is allocated on. Hence, if you can perform atomic operations on local
memory, you can make them on remote memory too. There are two ways in which these atomic operations are handled: 

1) **Remote Execution Atomic Operations**

	This is the most naive, but it is performed when nodes lack NICs like Aries which support network atomics
	at a hardware level, and is most commonly used when applications run locally. For example, imagine if the
	user were want to perform a 'wait-free' atomic operation, such as `fetchAdd` on some remote memory location.
	Without a NIC supporting network atomics, it boils down to the below...

	```chpl
	var _value : atomic int;
	on _value do _value.fetchAdd(1);
	```

	In this case, while it is technically wait-free as it is technically bounded by network latency, it must spawn 
	a remote task on the target node, and causes the current task to block until it returns. This is performed implicitly, but the performance penalty is severe enough to bottleneck any application. Furthermore, spawning a remote task deprives the target node of valuable resources, and as such results in degrading performance.

2) **Network Atomic Operations**

	This requires very specific hardware, such as the Aries NIC, which is Cray proprietary hardware and top-of-the-line.
	This is required for scalable (or even acceptable) performance for ordered data structures. Using the same example
	as before, a `fetchAdd` in this case is 'wait-free' enough to allow scalable performance. Scalable performance can
	only be achieved via an algorithm that is also bounded in terms of 'retry' operations, which rules out certain synchronization
	patterns, such as the lock-free 'CAS Retry loop' where the cost of retrying is too expensive, hence ruling out any lock-free
	algorithms and methodologies.

## Atomic Operations on 'Wide' Pointers

As memory can be accessed transparently from the user's perspective, it must be kept track of by the runtime. Hence, to determine
which node the memory belongs to, the pointer is 'widened' into a 128-bit value which keeps track of both the `addr` and the
`locale`. The next issue is that majority of hardware do not support 128-bit network atomic operations, even the Aries NIC. With
the second approach (above) ruled out in terms of a lack of hardware support, this only allows the first approach. However, as mentioned
before, this leads to degrading performance as such is not feasible as an actual solution.

One approach to solve the problem using the second approach is to use a technique called 'pointer compression', which takes advantage of
the fact that operating systems only makes use of the first 48 bits of the virtual address space, allowing the most significant 16 bits
to store the `locale`. This approach works very well for clusters with less than 2^16 nodes, but is a short-term solution and not fitting
for a language that prides itself on portability. 

My approach aims to solve the problem for any number of nodes. In my solution, I used descriptors to denote objects by id, and a table
to store said objects (the descrirptor being the index into the table). This way, we may use the second approach by 
performing the atomic operations using 64-bit descriptors. This approach, while scalable, is magnitudes slower than 'pointer compression' but will work for up 2^32 nodes. Furtermore, a more practical solution, involving the Chapel runtime, is planned to
significantly improve performance. As such, this project is deemed a 'Work-In-Progress'.

## Prerequisites

`GlobalAtomicObject` requires `BigInteger` module and hence requires GMP to be built (hence you may need to rebuild
Chapel with it if you do not already have it installed).