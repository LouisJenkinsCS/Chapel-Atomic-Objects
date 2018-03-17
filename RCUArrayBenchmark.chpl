use ConcurrentArray;
use Random;
use Time;
use BlockDist;
use IO;

config var numWrites = 0;
config var numTrials = 4;
config var outputFile = "";
config var target = "";
config var numOperations = 0;

config param maxSize = 1024 * 1024;

proc runRCUArray() {
  var results : [1..numTrials] real;
  var timer = new Timer();
  var array = new ConcurrentArray(int);
  var capacity : atomic int;

  array.expand(ConcurrentArrayChunkSize);
  capacity.write(ConcurrentArrayChunkSize);

  for i in 0 .. numTrials {
    timer.clear();
    timer.start();

    coforall loc in Locales  do on loc {
      coforall tid in 1..here.maxTaskPar {
        var rng = makeRandomStream(real(64));
        for ix in 1 .. numOperations {
          if numWrites >= abs(rng.getNext()) {
            // Write...
            if capacity.peek() < maxSize {
              array.expand(ConcurrentArrayChunkSize);
              capacity.fetchAdd(ConcurrentArrayChunkSize);
            }
          } else {
            // Read...
            var idx = ix % capacity.peek();
            array[idx] = idx;
          }
        }
      }
    }

    timer.stop();

    // Discard first run...
    if i == 0 then continue;
    results[i] = timer.elapsed();
  }
  var outfile = open(outputFile, iomode.cw);
  var outwriter = outfile.writer();
  outwriter.writeln((+ reduce results) / numTrials);
  outwriter.close();
  outfile.close();
}

proc main() {
  if numTrials == 0 then halt("numTrials(", numTrials, ") must be non-zero...");
  else if outputFile == "" then halt("outputFile(", outputFile, ") must be set...");
  else if target == "" then halt("target(", target, ") must be set...");
  else if numOperations == 0 then halt("numOperations(", numOperations, ") must be non-zero...");
  
  runRCUArray();
}