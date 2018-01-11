use DistributedVector;
use Random;
use Benchmark;
use Plot;
use BlockDist;

config param nElems = 1024 * 1024;

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
	var plotter : Plotter(int, real);
 	var targetLocales = (1,2,4,8,16,32,64);

 	var deinitFn = lambda(obj : object) {
		// coforall loc in Locales do on loc do delete (obj : VectorWrapper).vec._value;
  	};

  	runBenchmarkMultiplePlotted(
      benchFn = lambda(bd : BenchmarkData) {
        var vec = (bd.userData : VectorWrapper).vec;
        var randStream = makeRandomStream(uint);
		for ix in 1 .. bd.iterations {
			var idx = ((randStream.getNext() % max(nElems, 1) : uint)) : int;
			vec[idx] = idx;
		}
      },
      benchTime = 1,
      deinitFn = deinitFn,
      targetLocales=targetLocales,
      benchName = "DistVector",
      plotter = plotter,
      initFn = lambda (bmd : BenchmarkMetaData) : object {
        var wrapper = new VectorWrapper(new DistVector(int));
        wrapper.vec.expand(nElems);
        return wrapper;
      }
  	);

  	runBenchmarkMultiplePlotted(
      benchFn = lambda(bd : BenchmarkData) {
        var arrWrapper = (bd.userData : ArrayWrapper);
        var randStream = makeRandomStream(uint);
		for ix in 1 .. bd.iterations {
			arrWrapper.lock$ = true;

			var idx = ((randStream.getNext() % max(nElems, 1) : uint)) : int;
			arrWrapper.arr[idx] = idx;
			
			arrWrapper.lock$;
		}
      },
      benchTime = 1,
      deinitFn = deinitFn,
      targetLocales=targetLocales,
      benchName = "SyncArray",
      plotter = plotter,
      initFn = lambda (bmd : BenchmarkMetaData) : object {
        return new ArrayWrapper();
      }
  	);

  	runBenchmarkMultiplePlotted(
      benchFn = lambda(bd : BenchmarkData) {
        var arrWrapper = (bd.userData : ArrayWrapper);
        var randStream = makeRandomStream(uint);
		for ix in 1 .. bd.iterations {
			var idx = ((randStream.getNext() % max(nElems, 1) : uint)) : int;
			arrWrapper.arr[idx] = idx;
		}
      },
      benchTime = 1,
      deinitFn = deinitFn,
      targetLocales=targetLocales,
      benchName = "Array",
      plotter = plotter,
      initFn = lambda (bmd : BenchmarkMetaData) : object {
        return new ArrayWrapper();
      }
  	);

  	plotter.plot("DistVector_Benchmark");
}