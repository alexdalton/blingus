from optparse import OptionParser
import matplotlib.pyplot as plt
import os
from tabulate import tabulate

# Configuration variables
traces_dir = "/home/alex/blingus/CS533/traces/"  # directory with all the trace files
plots_dir = "/home/alex/blingus/CS533/plots/"
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
    TGIDAggTimes = {}
    PIDtoTGID = {}
    PIDtoTask = {}
    TGIDtoTasks = {}
    for i in xrange(numCores):
        cpuFreqs.append([])
        cpuFreqTimes.append([])
        cpuTasks.append([])
        cpuTaskTimes.append([])


    offset = None                  # The time offset (time of the first timestamp)

    for line in trace_fd:
        # ignore lines that begin with '#' (commented lines)
        if line.lstrip()[0] == "#":
            continue

        items = line.split(":")
        if len(items) < 3:
            #print("ERROR: {0}".format(line))
            continue

        taskPIDEndIndex = line.find(" (")
        if taskPIDEndIndex == -1:
            #print("ERROR: {0}".format(line))
            continue

        task_pid = line[0:taskPIDEndIndex].strip()
        pid = task_pid.split("-")[-1]
        task = task_pid[0:-len(pid) - 1]

        TGIDEndIndex = line.find(") ")
        TGIDStartIndex = taskPIDEndIndex + 2

        try:
            tgid = int(line[TGIDStartIndex:TGIDEndIndex])
        except ValueError:
            tgid = None

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

        PIDtoTGID[pid] = tgid
        PIDtoTask[pid] = task
        if tgid not in TGIDtoTasks:
            TGIDtoTasks[tgid] = set()
        TGIDtoTasks[tgid].add(task)

        # If the task is adbd set CPU to idle (ignore it)
        if (task == "adbd") or (task == "<idle>"):
            cpuState[cpuNum] = 0

        if function == "sched_switch":
            if line.find("==> next_comm=swapper") > -1:
                cpuState[cpuNum] = 0
                cpuFreqs[cpuNum].append(0)
                cpuFreqTimes[cpuNum].append(time)
            else:
                cpuState[cpuNum] = 1
        elif function.find("cpufreq") > -1:
            for item in line.split():
                if item[0:4] == "cur=":
                    cpuFreqs[cpuNum].append(int(item.split("=")[1]))
                    cpuFreqTimes[cpuNum].append(time)

        # If a pid is running on a different CPU now, make sure to set previous CPU's state to 0
        for i in xrange(numCores):
            if (not i == cpuNum) and (len(cpuTasks[i]) > 0) and (cpuTasks[i][-1] == pid):
                cpuState[i] = 0
                cpuFreqs[i].append(0)
                cpuFreqTimes[i].append(time)

        cpuTasks[cpuNum].append(pid)
        cpuTaskTimes[cpuNum].append(time)
        eventTimes.append(time)
        activeCPUs.append(cpuState.count(1))

    trace_fd.close()

    for i in xrange(8):
        trackPID = None
        trackPIDStart = None
        for j in xrange(len(cpuTasks[i])):
            currentPID = cpuTasks[i][j]
            currentTime = cpuTaskTimes[i][j]
            if trackPID is None or trackPIDStart is None:
                trackPID = currentPID
                trackPIDStart = currentTime
            elif trackPID == currentPID:
                continue
            else:
                trackTGID = PIDtoTGID[trackPID]
                if trackTGID is not None:
                    aggID = trackTGID
                else:
                    aggID = trackPID

                if aggID not in TGIDAggTimes:
                    TGIDAggTimes[aggID] = 0
                TGIDAggTimes[aggID] += currentTime - trackPIDStart

                trackPID = currentPID
                trackPIDStart = currentTime

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

    # plt.figure(1, figsize=(28, 15), dpi=80)
    # plt.plot(eventTimes, tlpTimes)
    # plt.ylabel("TLP")
    # plt.xlabel("Time (s)")
    # plt.title("TLP vs Time for {0}".format(file))
    # plt.savefig(plots_dir + "{0}_TLP_vs_Time.png".format(file), bbox_inches='tight')
    # plt.clf()

    plt.figure(2, figsize=(28, 15), dpi=80)
    for i in range(0, 8):
        plt.subplot(241 + i)
        if len(cpuFreqTimes[i]) > 0:
            cpuFreqTimes[i] = [0] + cpuFreqTimes[i] + [eventTimes[-1]]
            cpuFreqs[i] = [cpuFreqs[i][0]] + cpuFreqs[i] + [cpuFreqs[i][-1]]
            plt.axis([0, eventTimes[-1], 0, max(cpuFreqs[i]) + 100000])
            plt.plot(cpuFreqTimes[i], cpuFreqs[i], ls='steps')
        plt.ylabel("Frequency (kHz)")
        plt.xlabel("Time (s)")
        plt.title("CPU{0} Frequency vs. Time".format(i, file))

    plt.show()
    #plt.savefig(plots_dir + "{0}_CPUFreqs_vs_Time.png".format(file), bbox_inches='tight')
    plt.clf()

    # plt.figure(3, figsize=(28, 15), dpi=80)
    # plt.plot(eventTimes, activeCPUs, ls='steps')
    # plt.ylabel("# Active CPUs")
    # plt.xlabel("Time (s)")
    # plt.title("# Active CPUs vs Time for {0}".format(file))
    # plt.savefig(plots_dir + "{0}_ActiveCPUS_vs_Time.png".format(file), bbox_inches='tight')
    # plt.clf()

    x = []
    labels = []
    kernelSum = 0
    for k, v in TGIDAggTimes.iteritems():
        try:
            if not PIDtoTask[k] == "<...>":
                labels.append(PIDtoTask[k])
                x.append(v)
            else:
                kernelSum += v
        except KeyError:
            labels.append(str(TGIDtoTasks[k]))
            x.append(v)
    x.append(kernelSum)
    labels.append("Kernel")

    # plt.figure(4, figsize=(28, 15), dpi=80)
    # plt.pie(x, labels=labels)
    # plt.savefig(plots_dir + "{0}_Process_Piechart.png".format(file), bbox_inches='tight')
    # plt.clf()

# Print out TLPs in a nice format
print tabulate(tlps, headers=["Trace File", "TLP"])