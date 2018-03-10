# Chapel Software Transactional Memory

I was wondering whether or not you had any interest or input for a particular
STM-related project. While I can’t say that it will be added as a first-class
language feature (I.E language construct) it could definitely show potential
as a third-party library. I was wondering if you, as the expert in all things
STM, would be able to assist me in a few problems and design decisions.  For
now I’ll keep it short, but in general here is my proposal for it's design…

## Transactions using Task-specific Descriptors

As Chapel lacks any Task-Local or Thread-Local Storage, one crafty way around
it is to keep and recycle descriptors that hold the read and write logs.
Another plus to this approach is that tasks come into and out of existance
very rapidly, and using specific TLS would be very consuming to setup and
tear-down. Please ignore the lack of types for now...

```chpl
class STMDescr {
	// Similar to NORec STM
	var readSet;
	var writeSet;

	// Basically the 'snapshot' from NORec STM, doubles for Epoch-Based Reclamation
	var epoch;
	var globalEpochHandle;

	// Clean descriptor and setup for transaction
	proc begin();
	// Log write of 'val' into 'obj'
	proc write(obj, val);
	// Log read of 'obj', return copy
	proc read(obj) : val;
	// Commits transaction similar to NORec STM
	proc commit();
	// Maybe use reflection on the type to allocate the object's constructor for you?
	// Will handle cleaning up in case of abort operation...
	proc alloc(type objType, args...) : objType;
	// Make use of deleting memory only at a safe point in time using Epoch-Based Reclamation.
	// Safe to rollback...
	proc free(obj);
}

``` 

## Transaction Tracking and Epoch-Based Garbage Collection

By using a list of descriptors we can see which transactions are currently in-
use, which are not, and on which version of the data structure they are using.
Using the minimum 'epoch' for all active transactions we can safely determine
when it is safe to properly reclaim memory, that is the objects that are
marked for  garbage collection via a `free` call in a successful commit
operation.

## Word-sized Granularity

To allow for the user to perform certain reads and writes from and to fields,
I was thinking that instead of using object granularity, it would be best to
handle it in terms of the individual word values themselves. If the user wants
to read multiword data (such as a record which is similar to a C struct) we
could read the entirety of it into memory. For example...

```
record Example { var x : int; var y : real; var z : string; }
var e : Example;
// Later in transaction...
stm.write(e.x, stm.read(e.x) + 1); /* increment for &e.x */
// This would show the updated version of the field 'e.x'
var e_cpy = stm.read(e);
```

I was thinking that this can be achieved by making use of modified words in
the 'writeSet' and 'readSet'. This is relatively expensive but shouldn't be
too bad.

## Transactions using Error Handling

Recently some error handling has been introduced into Chapel, and so it is the
safest way to perform a ‘rollback’ operation. Syntactically it would look like
such…

```chpl
var success = false;
while !success {
	try! {
		stm.begin();
		stm.write(x, stm.read(x) + 1);
		stm.commit();
		success = true;
	} catch abort : STMAbort {
		select abort.status {
			when STM_ABORT {
				// Handle abort
			}
			when STM_RETRY {
				// Handle user retry or conflict
			}
		}
	}
}

```

Unfortunately there is no way to jump back to the top of the loop but it
should do for now. As well these try-catch blocks work even from remote tasks
(so if we spawn a task on another node the exception is propogated
appropriately).

## Flat vs Closed Nesting Transactions and Eager vs Lazy Validation

I'm wondering whether or not you knew of any standard means of implementing
the closed nesting transactional model otherwise using  the 'Flat' model as a
fallback. As well one big issue I can see coming up is that eager/pessimistic
validation may end up causing a lot of performance issues since it could
potentially be performing it on remote memory and so I'm wondering if it is
possible to make use of lazy/optimistic validation. Note that snapshotting the
entire data structure is out of the question as we should assume it is
possible for the size of such a data structure to be significantly greater in
size than that of which any single node could handle. Perhaps we can do
something similar to what Read-Log- Update did and allow transactions to
'peek' into the 'writeSet' of other active transactions to determine whether
or not they have recently changed anything. It is *much* cheaper to look into
the bounded set of other transactions on the same node than it would be to
perform a revalidation on remote memory (for *each* remote memory location and
*each time* a local transaction would have completed). I believe this would
help a lot in terms of performance.

## Distributed Software Transactional Memory (using wide-pointers)

I didn't realize this before but apparently OpenSHMEM replicates all updates
to 'global' memory  across all nodes in favor of persistency. **Chapel does
not do this** and instead allows remote access into another node's address
space by widening pointers from 8-bytes to 16-bytes to contain the identiifer
for the node, allowing actual scalable algorithms that scale with the number
of nodes you add... as well **Chapel is not just an SPMD** language (I noticed
this when I was talking about at University of Rochester, people assumed that
this was the case), so data structures and algorithms do not need to be in
lockstep and to have cluster-wide barriers, they can do their own thing. Hence
it is crucial that STM work in favor of this model to actually scale. Note
that this model would only be for cases where you *can't* make greater use of
locality, such as for a strongly-ordered data structure (like a distributed
priority queue). This is in contrast to mutual exclusion, so this is something
I believe that would be necessary eventually.

My idea is to have something along the lines of a single `Global` locking
mechanism that can work based on timeouts where a single  node can process as
many transactions as it wants and can even 'request' more time. I believe that
it is more beneficial to allow other nodes to 'revoke' or 'evict' a node of
their mutual exclusion if they exceed their timeout. As well it would be even
more beneficial to make use of a queue-lock (or ticket-lock) to ensure
fairness as we do want all nodes to eventually have their fair share.  It can
be linearized as the lock and timestamp can be hosted on a single node and so
can be used to allow for a busy node to perform as much work as it possibly
can;  the true killer for performance here would be communication, so this way
we only have conflicts coming from tasks that hosted on the very same node. As
well a single task should be elected for each node to handle acquiring the
lock as it would greatly reduce communication, contention, and to allow
transactions to busy-wait locally and yield more often (note: Chapel uses
cooperative scheduling).

```
global ticketLock;
global evokeTimestamp;
global dataStructure;

if isElectedTask {
	// Perform on the node that hosts the global data
	on ticketLock {
		var node = ticketLock.append(nodeId);
		while !node.isReady() {
			chpl_task_yield();
			// Check if we can revoke current lock owner.
			var timestamp = evokeTimestamp;
			var currentTimestamp = getCurrentTimestamp();
			if timestamp < currentTimestamp && CAS(&evokeTimestamp, timestamp, -1) {
				// Notify current owning node and waiters...
			}
		}
	}
}

```

You can request more time by keeping track of your current timestamp and
performing  a CAS to advance it forward... if there is a match. Other details
such as how a task is elected is implementation-specific detail.

## End Result: Scalable Distributed Software Transactional Memory

You have more experience than I do in STM, but I believe that this model
should outperform any attempt at using locks. It should be noted that right
now  there are no distributed lock implementations in Chapel, only those that
are  provided by the tasking layer and that hosted on a single node, resulting
in Out-Of-Memory crashes when too many nodes with too many tasks attempt to
contest for the same lock. In this way I believe it should not only work where
the current lock implementation fails, but it should offer nearly non-
degradeable performance as you increase the number of nodes (and of course
should scale as you increase the number of processors per node). Just wanted
to see if you had any desire in participating in this as a collaborative
effort.
