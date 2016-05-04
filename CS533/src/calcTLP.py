from optparse import OptionParser
import matplotlib.pyplot as plt
import os
from tabulate import tabulate

# Configuration variables
traces_dir = "/home/alex/blingus/CS533/traces/"  # directory with all the trace files
numCores = 8                                     # number of cores for this device


parser = OptionParser()
parser.add_option("-f", "--file", dest="filename", help="ftrace output text file name", metavar="FILE")

(options, args) = parser.parse_args()

# If no file option included do all the files in the trace directory
if not options.filename:
    files = os.listdir(traces_dir)
# Otherwise run the TLP analysis for just the given trace file
else:
    files = [options.filename]

tlps = []
for file in files:
    print("Running on trace file: {0}".format(file))
    trace_fd = open(os.path.join(traces_dir, file), "r")

    cpuState = [0] * numCores      # initialize all cpus to off
    eventTimes = []                     # array to hold timestamps
    activeCPUs = []                # array to hold number of active cpus at each timestamp

    cpuFreqs = []                  # Holds numCore arrays to record frequency of given core
    cpuFreqTimes = []              # Holds numCore arrays to record timestamps the frequency is taken at
    cpuTasks = []
    cpuTaskTimes = []
    taskAggTimes = {}
    for i in xrange(numCores):
        cpuFreqs.append([])
        cpuFreqTimes.append([])
        cpuTasks.append([])
        cpuTaskTimes.append([])


    offset = None                  # The time offset (time of the first timestamp)
    cpufunctions = set()
    counter = 0
    for line in trace_fd:
        # ignore lines that begin with '#' (commented lines)
        if line.lstrip()[0] == "#":
            continue

        items = line.split(":")
        if len(items) < 3:
            continue

        task_pid = line[0]
        for i in range(1, len(line)):
            if line[i - 1] == " " and (line[i] == "[" or line[i] == "("):
                break
            task_pid += line[i]

        pid = task_pid.split("-")[-1]
        task = task_pid[0:-len(pid) - 1]
        cpuNum = int(line[len(pid) + len(task):].split("[")[1][0:3])
        pid = int(pid)

        try:
            time = float(items[0].split()[-1])
            function = items[1].strip()
        except ValueError:
            time = float(items[1].split()[-1])
            function = items[2].strip()

        if offset is None:
            offset = time

        time -= offset

        # If the task is adbd set CPU to idle (ignore it)
        if (task == "adbd") or (task == "<idle>"):
            cpuState[cpuNum] = 0

        if function == "sched_switch":
            if (line.find("==> swapper") > -1) or (line.find("==> next_comm=swapper") > -1):
                cpuState[cpuNum] = 0
            else:
                cpuState[cpuNum] = 1
        elif function.find("cpufreq") > -1:
            for item in line.split():
                if item[0:4] == "cur=":
                    cpuFreqs[cpuNum].append(int(item.split("=")[1]))
                    cpuFreqTimes[cpuNum].append(time)

        cpuTasks[cpuNum].append(pid)
        cpuTaskTimes[cpuNum].append(time)
        eventTimes.append(time)
        activeCPUs.append(cpuState.count(1))

    trace_fd.close()

    for i in xrange(8):
        trackTask = None
        trackTaskStart = None
        for j in xrange(len(cpuTasks[i])):
            currentTask = cpuTasks[i][j]
            currentTime = cpuTaskTimes[i][j]
            if trackTask is None or trackTaskStart is None:
                trackTask = currentTask
                trackTaskStart = currentTime
            elif trackTask == currentTask:
                continue
            else:
                if trackTask not in taskAggTimes:
                    taskAggTimes[trackTask] = 0
                taskAggTimes[trackTask] += currentTime - trackTaskStart
                trackTask = currentTask
                trackTaskStart = currentTime

    tlpNumerator = 0
    tlpDenominator = 0
    tlpTimes = [0.0]
    for i in range(1, len(eventTimes)):
        if activeCPUs[i] != 0:  # Don't care about 0 active CPUs
            tlpNumerator += (activeCPUs[i] * (eventTimes[i] - eventTimes[i-1]))
            tlpDenominator += (eventTimes[i] - eventTimes[i-1])
            tlpTimes.append(float(tlpNumerator) / float(tlpDenominator))
        else:
            tlpTimes.append(tlpTimes[-1])

    tlps.append([file, float(tlpNumerator) / float(tlpDenominator)])

    plt.figure(1)
    plt.plot(eventTimes, tlpTimes)

    plt.figure(2)
    for i in range(0, 8):
        plt.subplot(241 + i)
        if len(cpuFreqTimes[i]) > 0:
            cpuFreqTimes[i] = [0] + cpuFreqTimes[i] + [eventTimes[-1]]
            cpuFreqs[i] = [cpuFreqs[i][0]] + cpuFreqs[i] + [cpuFreqs[i][-1]]
            plt.axis([0, eventTimes[-1], 0, max(cpuFreqs[i])])
            plt.plot(cpuFreqTimes[i], cpuFreqs[i], ls='steps')

    plt.figure(3)
    plt.plot(eventTimes, activeCPUs, ls='steps')

    x = []
    labels = []
    for k, v in taskAggTimes.iteritems():
        x.append(v)
        labels.append(k)

    plt.figure(4)
    plt.pie(x, labels=labels)
    plt.show()
# Print out TLPs in a nice format
print tabulate(tlps, headers=["Trace File", "TLP"])