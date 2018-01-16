use DistributedVector;
use Random;
use Time;
use BlockDist;

config param nElems = 1024 * 1024;
config param nIterationsPerTask = 1024 * 1024;
config param nTrials = 4;

class VectorWrapper {
  var vec : DistVector(int);
}

class ArrayWrapper {
	var space = {0..nElems};
	var dom = space dmapped Block(boundingBox=space);
	var arr : [dom] int;
	var lock$ : sync bool;
}

proc main() {
  var vec = new DistVector(int);
  vec.expand(nElems);
  var timer = new Timer();
  var results : [1..nTrials] real;
  for i in 0 .. nTrials {
    timer.clear();
    timer.start();
    coforall loc in Locales do on loc {
      coforall tid in 1..here.maxTaskPar {
        var randStream = makeRandomStream(uint);
        for ix in 1 .. nIterationsPerTask {
          var idx = ((randStream.getNext() % max(nElems, 1) : uint)) : int;
          vec[idx] = idx;
        }
      }
    }
    timer.stop();

    // Discard first run...
    if i == 0 then continue;
    results[i] = timer.elapsed();
  }
  writeln("[Vector]: ", (+ reduce results) / nTrials);
	
  var space = {0..nElems};
  var dom = space dmapped Block(boundingBox=space);
  var arr : [dom] int;
  var lock$ : sync bool;

  for i in 0 .. nTrials {
    timer.clear();
    timer.start();
    coforall loc in Locales do on loc {
      coforall tid in 1..here.maxTaskPar {
        var randStream = makeRandomStream(uint);
        for ix in 1 .. nIterationsPerTask {
          var idx = ((randStream.getNext() % max(nElems, 1) : uint)) : int;
          arr[idx] = idx;
        }
      }
    }
    timer.stop();

    // Discard first run...
    if i == 0 then continue;
    results[i] = timer.elapsed();
  }
  writeln("[Array]: ", (+ reduce results) / nTrials);

	for i in 0 .. nTrials {
    timer.clear();
    timer.start();
    coforall loc in Locales do on loc {
      coforall tid in 1..here.maxTaskPar {
        var randStream = makeRandomStream(uint);
        for ix in 1 .. nIterationsPerTask {
          lock$ = true;
          var idx = ((randStream.getNext() % max(nElems, 1) : uint)) : int;
          arr[idx] = idx;
          lock$;
        }
      }
    }
    timer.stop();

    // Discard first run...
    if i == 0 then continue;
    results[i] = timer.elapsed();
  }
  writeln("[Sync (Per Iteration) Array]: ", (+ reduce results) / nTrials);

  for i in 0 .. nTrials {
    timer.clear();
    timer.start();
    coforall loc in Locales do on loc {
      coforall tid in 1..here.maxTaskPar {
        var randStream = makeRandomStream(uint);
        lock$ = true;
        for ix in 1 .. nIterationsPerTask {
          var idx = ((randStream.getNext() % max(nElems, 1) : uint)) : int;
          arr[idx] = idx;
        }
        lock$;
      }
    }
    timer.stop();

    // Discard first run...
    if i == 0 then continue;
    results[i] = timer.elapsed();
  }
  writeln("[Sync (Per Task) Array]: ", (+ reduce results) / nTrials);
}