use DistributedVector;
use Random;
use Benchmark;
use Plot;

class VectorWrapper {
  var vec : DistVector(int);
}

class ArrayWrapper {
	var dom = {0..-1};
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
			var idx = ((randStream.getNext() % max((bd.iterations / 2), 1) : uint) + 1) : int;
			vec[idx] = idx;
		}
      },
      benchTime = 1,
      deinitFn = deinitFn,
      targetLocales=targetLocales,
      benchName = "DistVector",
      plotter = plotter,
      initFn = lambda (bmd : BenchmarkMetaData) : object {
        return new VectorWrapper(new DistVector(int));
      }
  	);

  	runBenchmarkMultiplePlotted(
      benchFn = lambda(bd : BenchmarkData) {
        var arrWrapper = (bd.userData : ArrayWrapper);
        var randStream = makeRandomStream(uint);
		for ix in 1 .. bd.iterations {
			arrWrapper.lock$ = true;

			var idx = ((randStream.getNext() % max((bd.iterations / 2), 1) : uint) + 1) : int;
			if arrWrapper.dom.high < idx {
				arrWrapper.dom = {0..idx};
			}
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

  	plotter.plot("DistVector_Benchmark");
}