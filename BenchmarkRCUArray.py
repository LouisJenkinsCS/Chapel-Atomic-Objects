import argparse
import numpy

numLocales = [1,2,4,8,16,32];
numTrials = 4;
numWrites = (numpy.array(range(0,11, 2)) * 10);
targets = ["QSBR", "EBR", "ChapelArray"]
numOperations = 1024 * 1024
fileName = ""

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

args = parser.parse_args()
numTrials = int(args.numTrials)
fileName = args.fileName
numOperations = args.numOperations

# Compile executable
print("chpl --fast " + fileName + ".chpl -o " + fileName);

# Execute

targetResults = {}
for target in targets:
	localeResults = {}
	for locales in numLocales:
		writeResults = {}
		for writes in numWrites:		
			outputFile = target + "-" + str(writes) + "-" + str(locales)
			
			# Execute 
			print("./" + fileName + " -nl " + str(locales) 
				+ " --numWrites=" + str(writes) + " --numTrials=" + str(numTrials)
				+ " --outputFile=" + outputFile + " --target=" + target 
				+ " --numOperations=" + str(numOperations))
			
			# Collect results...
			targetResult = 0;
			writeResults[writes] = targetResult
		localeResults[locales] = writeResults
	targetResults[target] = localeResults

print(targetResults)
for target in targetResults:
	outputStr = {0: "", 20: "", 40: "", 60: "", 80: "", 100: ""};
	for loc in targetResults[target]:
		for writes in targetResults[target][loc]:
			outputStr[writes] += str(targetResults[target][loc][writes]) + " "
	print (target + ": " + str(outputStr))

