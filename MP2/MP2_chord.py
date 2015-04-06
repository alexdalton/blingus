__author__ = 'Yanning Li'
# This code implements the chord protocol


import socket
import sys
import threading
import time
import cmd
import Queue
import csv
import random
import json
import numpy


# use global queues for message passing between threads
# g_msg[node_id] = q_node_id
g_msg = {}

# global variable for the dimension
g_dim = 8


# thread of each node
# the client thread which constantly read q_toSend and send out messages
class NodeThread(threading.Thread):
    def __init__(self, node_id):
        threading.Thread.__init__(self)
        self.node_id = node_id
        self.node_finger_table = numpy.zeros(g_dim, g_dim)
        self.keys = []
        self.is_alive = True

    def run(self):
        # print "Starting sending thread"

        while self.is_alive:
            # check global message queue
            pass

    def kill(self):
            self.is_alive = False


# command line interface thread
class CmdThread(threading.Thread):
    def __init__(self, name):
        threading.Thread.__init__(self)
        # self.threadID = thread_id
        self.name = name

    def run(self):
        # create command line interface object
        cli = MP2Shell()
        # print 'created cmd line interface object'
        cli.cmdloop()
        # print 'started command line interface'
        return True


# command line class
class MP2Shell(cmd.Cmd):
    intro = 'Welcome to the MP2 shell,' \
            ' type help or ? to list commands'
    prompt = '(MP2) '
    file = None

    # ------- basic commands -------------
    def do_join(self, arg):
        """
        join node p
        :param arg: node ID p
        :return:
        """
        tp = arg.split()
        if len(tp) < 1:
            print("not enough parameters")
            return

        # check if node exits

        # if not, create a new thread for node p

    def do_find(self, arg):
        """
        ask node p to locate key k
        :param arg: node id p; key id k
        :return:
        """
        tp = arg.split()
        if len(tp) < 2:
            print("not enough parameters")
            return

        # push command to q_node_p

        # wait for return value

    def do_leave(self, arg):
        """
        leave node p
        :param arg: node id p;
        :return:
        """
        tp = arg.split()
        if len(tp) < 1:
            print("not enough parameters")
            return

    def do_show(self, arg):
        """
        show keys stored in node p, or all
        :param arg: node id p, or 'all'
        :return:
        """
        tp = arg.split()
        if len(tp) < 1:
            print("not enough parameters")
            return


    def do_bye(self, arg):
        """
        :param arg: bye (stop code)
        :return: true
        """
        print 'stop demo'
        return True
        # sys.exit(0)

    def emptyline(self):
        # overwrite to not execute last command when hit enter
        pass


def main(argv):

    # initialize the chord by creating N0

    # start MP2Shell
    # MP2shell as the coordinator, creates and removes nodes.
    shell_thread = CmdThread('MP2Shell')
    print 'created shell thread'
    shell_thread.start()



if __name__ == '__main__':
    main(sys.argv[1:])