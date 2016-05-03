from optparse import OptionParser
import matplotlib.pyplot as plt
import os
from tabulate import tabulate

parser = OptionParser()
parser.add_option("-f", "--file", dest="filename", help="ftrace output text file name", metavar="FILE")

(options, args) = parser.parse_args()

# directory with all the trace files
traces_dir = "/home/alex/blingus/CS533/traces/"

# If no file option included do all the files in the trace directory
if not options.filename:
    files = os.listdir(traces_dir)
# Otherwise run the TLP analysis for just the given trace file
else:
    files = [options.filename]

tlps = []
for file in files:
    trace_fd = open(os.path.join(traces_dir, file), "r")

    cpuState = [0] * 8      # initialize all cpus to off
    times = []              # array to hold timestamps
    activeCPUs = []         # array to hold number of active cpus at each timestamp
    cpuFreq = [[] for k in xrange(8)]
    cpuFreqTimes = [[] for j in xrange(8)]

    for line in trace_fd:
        # ignore lines that begin with '#' (commented lines)
        if line.lstrip()[0] == "#":
            continue

        # split line by whitespace
        items = line.split()

        try:
            pid = items[0].split("-")[-1]
            task = items[0][0:-1 - len(pid)]
            cpu = None
            time = None
            function = None

            # Case for first type of trace file
            if items[1][0] == "[":
                cpu = int(items[1].strip("[]"))     # get CPU number from line
                time = float(items[2].rstrip(":"))  # get timestamp from line
                function = items[3].rstrip(":")
            # Case for second type of trace file
            elif items[2][0] == "[":
                cpu = int(items[2].strip("[]"))     # get CPU number from line
                time = float(items[4].rstrip(":"))  # get timestamp from line
                function = items[5].rstrip(":")
        except ValueError:
            continue
        except IndexError:
            continue

        # Catch for incorrectly formatted line
        if cpu is None or time is None or function is None:
            continue

        # If the task is adbd set CPU to idle (ignore it)
        if task == "adbd":
            cpuState[cpu] = 0
            # cpuFreqTimes[cpu].append(time)
            # cpuFreq[cpu].append(0)
        # If we have a sched_switch task and going to the swapper set to idle
        elif function == "sched_switch":
            if line.find("==> swapper") != -1:
                cpuState[cpu] = 0
                cpuFreqTimes[cpu].append(time)
                cpuFreq[cpu].append(0)
            if line.find("==> next_comm=swapper"):
                cpuState[cpu] = 0
                cpuFreqTimes[cpu].append(time)
                cpuFreq[cpu].append(0)
        elif function[0:7] == "cpufreq":
            cpuFreqInfo = line.split(':')[2]
            for info in cpuFreqInfo.split():
                if info[0:4] == "cur=":
                    cpuFreq[cpu].append(int(info[4:]))
                    cpuFreqTimes[cpu].append(time)
        # Otherwise CPU is active at this time
        else:
            cpuState[cpu] = 1

        times.append(time)
        activeCPUs.append(cpuState.count(1))

    trace_fd.close()

    offset = times[0]       # First time stamp is amount to offset each time by
    for cpuFreqTime in cpuFreqTimes:
        for i in range(0, len(cpuFreqTime)):
            cpuFreqTime[i] -= offset

    tlpNumerator = 0
    tlpDenominator = 0
    times[0] = 0.0          # Make sure we set first timestamp to 0

    tlpTimes = [0.0]
    for i in range(1, len(times)):
        times[i] -= offset      # Offset by first timestamp
        if activeCPUs[i] != 0:  # Don't care about 0 active CPUs
            tlpNumerator += (activeCPUs[i] * (times[i] - times[i-1]))
            tlpDenominator += (times[i] - times[i-1])
        try:
            tlpTimes.append(float(tlpNumerator) / float(tlpDenominator))
        except ZeroDivisionError:
            tlpTimes.append(0.0)

    tlps.append([file, float(tlpNumerator) / float(tlpDenominator)])

    plt.figure(1)
    for i in range(0, 8):
        plt.subplot(241 + i)
        if len(cpuFreqTimes[i]) > 0:
            plt.axis([0, times[-1], 0, max(cpuFreq[i])])
            plt.plot(cpuFreqTimes[i], cpuFreq[i], ls='steps')

    plt.show()

    # Uncomment to plot Active CPUs vs Time
    # plt.plot(times, tlpTimes)
    # plt.ylabel("# of Active CPUs")
    # plt.xlabel("Time (s)")
    # plt.show()

# Print out TLPs in a nice format
print tabulate(tlps, headers=["Trace File", "TLP"])