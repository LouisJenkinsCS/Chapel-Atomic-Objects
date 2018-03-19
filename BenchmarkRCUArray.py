import threading
import argparse
import time
import numpy
import random
import Queue
import os
from subprocess import Popen, call

processQueue = Queue.Queue()
processWorkerKeepAlive = True
processWorkerTimeout = 1 # seconds

class Task:
	isDone = False
	args = []

	# Used for parallelization
	localesNeeded = 0

	def __init__(self, args, localesNeeded):
		self.args = args
		self.localesNeeded = localesNeeded

# Worker Thread
def processWorker():
	activeProcs = []
	FNULL = open(os.devnull, 'w')
	while processWorkerKeepAlive:
		# Check current processes...
		for (task, proc) in activeProcs:
			if proc.poll() is not None:
				task.isDone = True;
				activeProcs.remove((task, proc));

		try:
			# Accepts arguments to spawn a process
			# As this acquires a lock, it will flush our writes of the above as well
			task = processQueue.get(True, processWorkerTimeout)
		except Queue.Empty:
			continue

		# Handle processing...
		activeProcs.append((task, Popen(task.args, stdout=FNULL)))
		
maxLocales = 64
numLocales = [1,2,4,8,16,32]
numTrials = 4
numWrites = (0, 1, 10, 100, 1000, 10000)
targets = ["QSBR"] # ["QSBR", "EBR"]
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

parser.add_argument('--maxLocales',
	action='store',
	default=maxLocales,
	dest='maxLocales',
	help='Maximum amount of nodes to schedule for.'
)

args = parser.parse_args()
numTrials = int(args.numTrials)
fileName = args.fileName
numOperations = args.numOperations
datFile = args.datFile
maxLocales = args.maxLocales

EBRExecutable = fileName + "-EBR"
QSBRExecutable = fileName + "-QSBR"

# Start background thread
threading.Thread(target=processWorker).start()


# Compile executable
#print("Creating " + EBRExecutable + "...")
print("Creating " + QSBRExecutable + "...")
#task1 = Task(["chpl", "--fast", fileName + ".chpl", "-o", EBRExecutable], 0)
task2 = Task(["chpl", "--fast", fileName + ".chpl", "-sConcurrentArrayUseQSBR=1", "-o", QSBRExecutable], 0)
#processQueue.put(task1)
processQueue.put(task2)

while not task2.isDone: # or not task1.isDone:
	time.sleep(1)

# Execute
# TODO: Find a way to parallelize both EBR and QSBR...
results = {}
tasks = []
localesUsed = 0
targetResults = {}
outputFiles = {}

# Initialize the maps used above
for target in targets:
	outputFiles[target] = {}
	targetResults[target] = {}
	for locales in numLocales:
		outputFiles[target][locales] = {}
		targetResults[target][locales] = {}

for locales in numLocales:		
	for writes in numWrites:
		for target in targets:
			# Store output file as way to obtaining result from this task
			outputFile = target + "-" + str(writes) + "-" + str(locales) + ".result"
			outputFiles[target][locales][writes] = outputFile

			print("Target=" + target + ", Writes=" + str(writes) + ", Locales=" + str(locales))

			# Submit to task queue
			executable = EBRExecutable if target == "EBR" else QSBRExecutable
			task = Task(["../chapel/util/test/chpl_launchcmd.py", "--walltime=01:00:00", "./" + executable,  "-nl", str(locales), 
				"--numCheckpoints", str(writes), "--numTrials", str(numTrials),
				"--outputFile", outputFile, "--target", target, 
				"--numOperations", str(numOperations)], locales)
			processQueue.put(task)
			tasks.append(task)
			localesUsed += locales;

			# Wait for current running processes if at max capacity
			while localesUsed >= maxLocales:
				time.sleep(1)
				for t in tasks:
					if t.isDone:
						tasks.remove(t)
						localesUsed -= t.localesNeeded
		
# Wait for current tasks
while localesUsed != 0:
	time.sleep(1)
	for t in tasks:
		if t.isDone:
			tasks.remove(t)
			localesUsed -= locales

# Shutdown worker
processWorkerKeepAlive = False

# Collect results from files...
for target in outputFiles:
	for locales in outputFiles[target]:
		for writes in outputFiles[target][locales]:
			outputPath = outputFiles[target][locales][writes]
			if os.path.exists(outputPath):
				outputFile = open(outputPath, "r")
				output = outputFile.read()
				if output == "":
					targetResults[target][locales][writes] = 0
				else:
					targetResults[target][locales][writes] = float(output)
			else:
				targetResults[target][locales][writes] = 0

# Translate output from files into space-separated files
targetOutput = {}
for target in targetResults:
	buf = ""
	for loc in sorted(targetResults[target].keys()):
		for writes in sorted(targetResults[target][loc].keys()):
			buf += str(loc) + " " + str(writes) + "% " + str(targetResults[target][loc][writes]) + "\n"
	targetOutput[target] = buf

# Write .dat files
files = {}
for t in targetOutput.keys():
	fname = t + "-" + datFile
	file = open(fname, "w+")
	for v in targetOutput[t]:
		file.write(v)
	file.close()
	files[t] = fname

# Create GNUPlot
gplot = open("tmp.gplot", "w+")
gplot.write("set term pngcairo\n")
gplot.write("set output 'out.png'\n")
gplot.write("set dgrid3d 30,30\n")
gplot.write("set hidden3d\n")
gplot.write("set ylabel \"ops per checkpoints\"\n")
gplot.write("set xlabel \"locales\"\n")
gplot.write("set logscale x 2\n")
plotStr = "splot "
plotStrArr = []
for target in files:
	plotStrArr.append("\"" + files[target] + "\" using 1:2:3 title \"" + target + "\" with lines")
plotStr += ", ".join(plotStrArr)
gplot.write(plotStr)
gplot.close()

call(["gnuplot", "tmp.gplot"])
