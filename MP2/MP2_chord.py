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
import math


# use global queues for message passing between threads
# g_msg[node_id] = q_msg
# q_msg:
# ('shell input message', 'nodes message')
g_msg = {}

# global variable for the dimension
g_dim = 8


# thread of each node
# the client thread which constantly read q_toSend and send out messages
class NodeThread(threading.Thread):
    def __init__(self, node_id):
        threading.Thread.__init__(self)
        self.node_id = node_id

        # (start, (interval), successor)
        self.finger = list()

        # successor(finger[1]); initialized as 0
        self.successor = 0

        # save all the keys in a list of integers
        self.keys = []

        self.is_alive = True

    def run(self):
        print "created node {0}".format(self.node_id)

        while self.is_alive:
            self.parse_msg_queue()
            time.sleep(0.1)

    def kill(self):
        self.is_alive = False

    # initialize with all 256 key values
    def init_node_0(self):
        pass


    # parse the queue and call respective functions
    def parse_msg_queue(self):

        if not g_msg[self.node_id].empty():
            msg_tp = g_msg[self.node_id].get()

            cmd_msg = msg_tp[0]
            tp = cmd_msg.split()

            if tp[0] == 'join':
                # join it self to the ring
                pass
            elif tp[0] == 'find':
                # double check if the msg is correctly pushed in the right queue
                if int(tp[1]) != self.node_id:
                    print 'message {0} pushed to wrong queue {1} \n'.format(cmd_msg, self.node_id)
                else:
                    key_id = int(tp[2])
                    self.find_successor(cmd_msg, key_id)
            elif tp[0] == 'leave':
                # leave the ring
                pass
            elif tp[0] == 'show':
                # show the keys that it saved
                print 'Node {0} stores keys {1} \n'.format(self.node_id, self.keys)


    # find successor
    def find_successor(self, cmd_msg, key_id):
        if self.node_id < key_id <= self.successor:
            # found
            print 'Key {0} saved on node {1} \n'.format(key_id, self.successor)
        else:
            closest_node_id = self.closest_preceding_node(key_id)
            # push to the queue of the node
            g_msg[closest_node_id].put((cmd_msg, ''))


    # find the closest preceding nodes in this table for id
    def closest_preceding_node(self, key_id):
        cnt = g_dim
        while cnt >= 0:
            succ_node = self.finger[cnt][2]

            # may go around the circle
            if id <= self.node_id:
                tmp_id = key_id + math.pow(2, g_dim)
                tmp_succ_node = succ_node + math.pow(2, g_dim)
            else:
                tmp_id = key_id
                tmp_succ_node = succ_node

            # compare and find the closest node id
            if self.node_id < tmp_succ_node < tmp_id:
                return succ_node


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
        if int(tp[1]) in g_msg.keys():
            pass

        else:
            node_id = int(tp[1])
            # if not, create a new thread for node p
            node_name = 'n_{0}'.format(node_id)
            vars()[node_name] = NodeThread(node_id)
            print 'created node n_{0}\n'.format(node_id)
            # start thread
            vars()[node_name].start()

            # push joint function in to message queue
            g_msg[node_id] = Queue.Queue()
            g_msg[node_id].put((arg, ''))

    def do_find(self, arg):
        """
        ask node p to locate key k
        :param arg: node id p; key id k
        :return:
        """
        tp = arg.split()
        if len(tp) < 2:
            print("not enough parameters \n")
            return

        # push command to q_node
        node_id = int(tp[1])
        if node_id not in g_msg.keys():
            print 'Error, node {0} does not exist \n'.format(node_id)
        else:
            g_msg[node_id].put((arg, ''))

        # node_id will print out results once found

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

        # put to queue
        node_id = int(tp[1])
        if node_id not in g_msg.keys():
            print 'Error, node {0} does not exist \n'.format(node_id)
        else:
            g_msg[node_id].put((arg, ''))

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

        if tp[1] == 'all':
            # push to all queues
            # each node will print out one table showing its info
            for key in g_msg:
                g_msg[key].put((arg, ''))
        else:
            node_id = int(tp[1])
            if node_id not in g_msg.keys():
                print 'Error, node {0} does not exist \n'.format(node_id)
            else:
                g_msg[node_id].put((arg, ''))

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
    n_0 = NodeThread(0)
    n_0.init_node_0()

    # start MP2Shell
    # MP2shell as the coordinator, creates and removes nodes.
    shell_thread = CmdThread('MP2Shell')
    print 'created shell thread'
    shell_thread.start()


if __name__ == '__main__':
    main(sys.argv[1:])