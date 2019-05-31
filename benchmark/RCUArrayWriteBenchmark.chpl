use RCUArray;
use Random;
use Time;
use BlockDist;

config param nElems = 1024 * 1024;
config param nTrials = 4;

proc main() {
  var csvTime : string;

 
  var timer = new Timer();
  var results : [1..nTrials] real;
  for i in 0 .. nTrials {
    var array = new RCUArray(int);
    timer.clear();
    timer.start();
    for 1 .. nElems / RCUArrayBlockSize {
      array.expand(RCUArrayBlockSize);
    }
    timer.stop();

    // Discard first run...
    if i == 0 then continue;
    results[i] = timer.elapsed();
  }
  writeln("[RCU Array]: Time=", ((+ reduce results) / nTrials));
  csvTime += ((+ reduce results) / nTrials) : string;

  for i in 0 .. nTrials {
    var space = {0..0};
    var dom = space dmapped Block(boundingBox=space);
    var arr : [dom] int;

    timer.clear();
    timer.start();
    var sz = 0;
    for 1 .. nElems / RCUArrayBlockSize {
      sz += RCUArrayBlockSize;
      dom = {1..sz};
    }
    timer.stop();

    // Discard first run...
    if i == 0 then continue;
    results[i] = timer.elapsed();
  }
  writeln("[Array]: Time=", ((+ reduce results) / nTrials));
  csvTime += ", " + ((+ reduce results) / nTrials) : string;
  
  writeln(csvTime);
}
