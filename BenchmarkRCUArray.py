import threading
import argparse
import time
import numpy
import random
import Queue
from subprocess import Popen, call

processQueue = Queue.Queue()
processWorkerKeepAlive = True
processWorkerTimeout = 1 # seconds

class Task:
	isDone = False
	args = []

	def __init__(self, args):
		self.args = args;

# Worker Thread
def processWorker():
	activeProcs = []
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
		activeProcs.append((task, Popen(task.args)))
		

numLocales = [1,2,4,8,16,32]
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

# Start background thread
threading.Thread(target=processWorker).start()


# Compile executable
print("Creating " + EBRExecutable + "...")
print("Creating " + QSBRExecutable + "...")
task1 = Task(["chpl", "--fast", fileName + ".chpl", "-o", EBRExecutable])
task2 = Task(["chpl", "--fast", fileName + ".chpl", "-sConcurrentArrayUseQSBR=1", "-o", QSBRExecutable])
processQueue.put(task1)
processQueue.put(task2)

while not task1.isDone or not task2.isDone:
	time.sleep(1)

# Execute
targetResults = {}
for target in targets:
	localeResults = {}
	for locales in numLocales:
		# TODO: Parallelize this part to handle waiting on processes in parallel...
		tasks = []
		localesUsed = 0
		writeResults = {}
		outputFiles = {}
		for writes in numWrites:		
			outputFile = target + "-" + str(writes) + "-" + str(locales)
			outputFiles[writes] = outputFile

			# Execute 
			executable = EBRExecutable if target == "EBR" else QSBRExecutable
			print("Executing " + executable + ", Writes=" + str(writes) + ", Locales=" + str(locales) + "\n")
			task = Task(["../chapel/util/test/chpl_launchcmd.py", "./" + executable,  "-nl", str(locales), 
				"--numWrites", str(writes), "--numTrials", str(numTrials),
				"--outputFile", outputFile, "--target", target, 
				"--numOperations", str(numOperations)])
			
			# Submit new process
			processQueue.put(task)
			tasks.append(task)
			localesUsed += locales;

			# Wait for current running processes
			while localesUsed == 32:
				time.sleep(1)
				for t in tasks:
					if t.isDone:
						tasks.remove(t)
						localesUsed -= locales
		
		# Wait for current tasks
		while localesUsed != 0:
			time.sleep(1)
			for t in tasks:
				if t.isDone:
					tasks.remove(t)
					localesUsed -= locales

		# Collect results...
		for w in outputFiles:
			outputFile = open(outputFiles[w], "r")
			output = outputFile.read()
			targetResult = float(output)
			writeResults[w] = targetResult
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
