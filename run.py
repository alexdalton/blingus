#!/usr/bin/env python
from optparse import OptionParser
from fuzzer import fuzzer

# Options for running the fuzzer
parser = OptionParser()
parser.add_option("-s", "--secs", dest="seconds",
                  help="seconds to fuzz for", default=10)
parser.add_option("-m", "--mins", dest="minutes",
                  help="minutes to fuzz for", default=0)
parser.add_option("-o", "--hours", dest="hours",
                  help="hours to fuzz for", default=0)
parser.add_option("-i", "--interface", dest="interface",
                  help="interface class to use while fuzzing")
parser.add_option("-f", "--fuzzfile", dest="fuzzFile",
                  help="file containing the fuzz vectors", metavar="FILE",
                  default="fuzzvectors")
parser.add_option("-a", "--mode", dest="mode",
                  help="fuzzing mode", default=0)
parser.add_option("-e", "--seed", dest="seed",
                  help="seed to use for random mutating")
(options, args) = parser.parse_args()

# Make sure we get the sender class
if options.interface is None:
    print("No interface class supplied")
    exit(1)

# Fuzz for the given amount of time
runTime = int(options.seconds) + (60 * int(options.minutes)) + (3600 * int(options.hours))
fuzzer = fuzzer(options.interface, options.fuzzFile, options.seed, options.mode)
fuzzer.fuzz(runTime)
