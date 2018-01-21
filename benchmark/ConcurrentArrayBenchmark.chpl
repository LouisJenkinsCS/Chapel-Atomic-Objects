use ConcurrentArray;
use Random;
use Time;
use BlockDist;

config param nElems = 1024 * 1024;
config param nIterationsPerTask = 1024 * 1024;
config param nTrials = 4;

proc main() {
  var csvTime : string;

  var array = new ConcurrentArray(int);
  array.expand(nElems);
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
          array[idx] = idx;
        }
      }
    }
    timer.stop();

    // Discard first run...
    if i == 0 then continue;
    results[i] = timer.elapsed();
  }
  writeln("[Concurrent Array]: ", "Op/Sec=", (nIterationsPerTask * here.maxTaskPar * numLocales) / ((+ reduce results) / nTrials), ", Time=", ((+ reduce results) / nTrials));
	csvTime += ((+ reduce results) / nTrials) : string;

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
  writeln("[Array]: ", "Op/Sec=", (nIterationsPerTask * here.maxTaskPar * numLocales) / ((+ reduce results) / nTrials), ", Time=", ((+ reduce results) / nTrials));
  csvTime += ", " + ((+ reduce results) / nTrials) : string;

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
  writeln("[Sync Array]: ", "Op/Sec=", (nIterationsPerTask * here.maxTaskPar * numLocales) / ((+ reduce results) / nTrials), ", Time=", ((+ reduce results) / nTrials));
  csvTime += ", " + ((+ reduce results) / nTrials) : string;

  writeln(csvTime);
}