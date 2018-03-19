use ConcurrentArray;
use Random;
use Time;
use BlockDist;
use IO;

config var numCheckpoints = 0;
config var numTrials = 4;
config var outputFile = "";
config var numOperations = 0;

config param maxSize = 1024 * 1024;

proc runRCUArray() {
  var results : [1..numTrials] real;
  var timer = new Timer();
  var array = new ConcurrentArray(int);

  // Do (X=Locales, Y=NCheckpoints, Z=Op/Sec)
  array.expand(maxSize);

  for i in 0 .. numTrials {
    timer.clear();
    timer.start();

    coforall loc in Locales  do on loc {
      coforall tid in 1..here.maxTaskPar {
        var rng = makeRandomStream(real(64), parSafe = false);
        for ix in 0 .. #numOperations {
          // Read...
          var idx = ix % maxSize;
          array[idx] = idx;

          if numCheckpoints > 0 && (ix % numCheckpoints) == 0 {
            // Invoke checkpoint
            extern proc chpl_qsbr_checkpoint();
            chpl_qsbr_checkpoint();
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
  outwriter.writeln((numOperations * here.maxTaskPar * numLocales) / ((+ reduce results) / numTrials));
  outwriter.close();
  outfile.close();
}

proc main() {
  if numTrials == 0 then halt("numTrials(", numTrials, ") must be non-zero...");
  else if outputFile == "" then halt("outputFile(", outputFile, ") must be set...");
  else if numOperations == 0 then halt("numOperations(", numOperations, ") must be non-zero...");
  runRCUArray();
}