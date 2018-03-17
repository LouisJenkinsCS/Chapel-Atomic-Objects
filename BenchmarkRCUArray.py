import argparse
import numpy
import random
from subprocess import call

# numLocales = [1,2,4,8,16,32]
numLocales = [1]
numTrials = 4
numWrites = (numpy.array(range(0,11, 2)) * 10)
targets = ["QSBR", "EBR"]
numOperations = 1024 * 1024
fileName = ""
datFile = "out.dat"

parser = argparse.ArgumentParser(description='Runs benchmark on Cray-XC 50.')
parser.add_argument('fileName', 
	metavar='fileName', 
	type=str, 
	action="store",
	help='The filename of the benchmark to run.'
)
parser.add_argument('--numTrials', 
	action='store',
    default=numTrials,
    dest="numTrials",
    help='Number of trials to run each benchmark (default: 4 trials)'
)
parser.add_argument('--numOperations',
	action='store',
	default=numOperations,
	dest='numOperations',
	help='Number of operations per run (default: ' + str(numOperations) + ' operations)'
)

parser.add_argument('--datFile',
	action='store',
	default=datFile,
	dest='datFile',
	help='File to print output to.'
)

args = parser.parse_args()
numTrials = int(args.numTrials)
fileName = args.fileName
numOperations = args.numOperations
datFile = args.datFile

EBRExecutable = fileName + "-EBR"
QSBRExecutable = fileName + "-QSBR"


# Compile executable
call(["chpl", "--fast", fileName + ".chpl", "-o", EBRExecutable]);
call(["chpl", "--fast", fileName + ".chpl", "-sConcurrentArrayUseQSBR=1", "-o", QSBRExecutable])

# Execute

targetResults = {}
for target in targets:
	localeResults = {}
	for locales in numLocales:
		writeResults = {}
		for writes in numWrites:		
			outputFile = target + "-" + str(writes) + "-" + str(locales)
			
			# Execute 
			executable = EBRExecutable if target == "EBR" else QSBRExecutable
			call(["./" + executable,  "-nl", str(locales), 
				"--numWrites", str(writes), "--numTrials", str(numTrials),
				"--outputFile", outputFile, "--target", target, 
				"--numOperations", str(numOperations)])
			
			# Collect results...
			outputFile = open(outputFile, "r")
			output = outputFile.read()
			print(output)
			targetResult = float(output)
			writeResults[writes] = targetResult
		localeResults[locales] = writeResults
	targetResults[target] = localeResults

targetOutput = {}
for target in targetResults:
	buf = ""
	for loc in sorted(targetResults[target].keys()):
		for writes in sorted(targetResults[target][loc].keys()):
			buf += str(loc) + " " + str(writes) + " " + str(targetResults[target][loc][writes]) + "\n"
	targetOutput[target] = buf

# NumLocales Write-Ratio Ops/Sec
files = []
for t in targetOutput.keys():
	fname = t + "-" + datFile
	file = open(fname, "w+")
	for v in targetOutput[t]:
		file.write(v);
	file.close()
	files.append(fname)

gplot = open("tmp.gplot", "w+")
gplot.write("set term pngcairo\n")
gplot.write("set output 'out.png'\n")
gplot.write("set dgrid3d 30,30\n")
gplot.write("set hidden3d\n")
plotStr = "splot "
for file in files:
	plotStr += "\"" + file + "\" u 1:2:3 with lines, "
gplot.write(plotStr)
gplot.close()

call(["gnuplot", "tmp.gplot"])
