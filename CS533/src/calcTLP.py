from optparse import OptionParser
import matplotlib.pyplot as plt
import os

parser = OptionParser()
parser.add_option("-f", "--file", dest="filename", help="ftrace output text file", metavar="FILE")

(options, args) = parser.parse_args()

traces_dir = "/home/alex/blingus/CS533/traces/"

if not options.filename:
    files = os.listdir(traces_dir)
else:
    files = [options.filename]

for file in files:
    trace_fd = open(os.path.join(traces_dir, file), "r")

    cpuState = [0] * 8
    times = []
    activeCPUs = []

    for line in trace_fd:
        # ignore comments
        if line.lstrip()[0] == "#":
            continue

        items = line.split()

        try:
            pid = items[0].split("-")[-1]
            task = items[0][0:-1 - len(pid)]
            cpu = None
            time = None
            function = None
            if items[1][0] == "[":
                cpu = int(items[1].strip("[]"))     # get CPU number from line
                time = float(items[2].rstrip(":"))  # get timestamp from line
                function = items[3].rstrip(":")
            elif items[2][0] == "[":
                cpu = int(items[2].strip("[]"))     # get CPU number from line
                time = float(items[4].rstrip(":"))  # get timestamp from line
                function = items[5].rstrip(":")
        except ValueError:
            continue
        except IndexError:
            continue

        if cpu is None or time is None or function is None:
            continue

        if task == "adbd":
            cpuState[cpu] = 0
        elif function == "sched_switch":
            if line.find("==> swapper") != -1:
                cpuState[cpu] = 0
            if line.find("==> next_comm=swapper"):
                cpuState[cpu] = 0
        else:
            cpuState[cpu] = 1

        times.append(time)
        activeCPUs.append(cpuState.count(1))

    top = 0
    bottom = 0
    for i in range(0, len(times)):
        top += activeCPUs[i] * times[i]
        if activeCPUs[i] != 0:
            bottom += times[i]

    print file, float(top) / float(bottom)

    plt.plot(times, activeCPUs, ls='steps')
