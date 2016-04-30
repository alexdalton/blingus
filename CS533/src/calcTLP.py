from optparse import OptionParser

parser = OptionParser()
parser.add_option("-f", "--file", dest="filename", help="ftrace output text file", metavar="FILE")

(options, args) = parser.parse_args()

trace_fd = open(options.filename, "r")

cpuState = [0] * 8
times = [0]
activeCPUs = [0]

for line in trace_fd:
    items = line.split()

    try:
        cpu = int(items[1].strip("[]"))
        time = float(items[2].rstrip(":"))
    except ValueError:
        continue
    except IndexError:
        continue

    if line.find("==> swapper") != -1:
        cpuState[cpu] = 0
    else:
        cpuState[cpu] = 1

    times.append(time)
    activeCPUs.append(cpuState.count(1))
    print times[-1], activeCPUs[-1]