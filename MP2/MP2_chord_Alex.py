__author__ = 'Yanning Li and Alex Dalton'
# This code implements the chord protocol


import socket
import sys
import threading
import time
import cmd
import Queue
import json
#import numpy
import math


# use global queues for message passing between threads
# g_msg[node_id] = q_msg
# q_msg:
# ('shell input message', 'nodes message')
g_msg = {}

# global variable for the dimension
g_dim = 8

# global dictionary to store thread handles indexed by node ID
n_threads = {}

class message():
    def __init__(self, from_id, to_id, msg, msg_args=[], ackID=None):
        self.from_id = from_id
        self.to_id = to_id
        self.msg = msg
        self.msg_args = msg_args
        self.ackID = ackID

    def send(self):
        print "send " + str([self.from_id, self.to_id, self.msg, self.msg_args])
        g_msg[self.to_id].put([self.from_id, self.msg, self.msg_args])

    def send_and_get_response(self):
        print "send " + str([self.from_id, self.to_id, self.msg, self.msg_args])
        g_msg[self.to_id].put([self.from_id, self.msg, self.msg_args])

        response = g_msg[self.from_id].get(block=True)
        while response[1] != self.ackID:
            g_msg[self.from_id].put(response)
            response = g_msg[self.from_id].get(block=True)
        return response[2]


# thread of each node
# the client thread which constantly read q_toSend and send out messages
class NodeThread(threading.Thread):
    def __init__(self, node_id):
        threading.Thread.__init__(self)
        self.node_id = node_id

        # (start, (interval), successor)
        # defined same as in the paper
        self.finger = []

        # successor(finger[1]); initialized as 0
        self.successor = 0
        self.predecessor = 0

        # save all the keys in a list of integers
        self.keys = []

        self.is_alive = True

        # create message queue for this node
        g_msg[node_id] = Queue.Queue()

    def run(self):
        #print "created node {0}".format(self.node_id)

        while self.is_alive:
            self.parse_msg_queue()
            time.sleep(0.1)

    def kill(self):
        self.is_alive = False

    def join(self):
        if self.node_id != 0:
            self.init_finger_table()
            self.update_others()
        else:
            self.keys = range(0, pow(2, g_dim))
            for i in range(0, g_dim):
                self.finger.append([(self.node_id + pow(2, i)) % pow(2, g_dim),
                                    [(self.node_id + pow(2, i)) % pow(2, g_dim), (self.node_id + pow(2, i + 1)) % pow(2, g_dim)],
                                    0])

    def init_finger_table(self):
        for i in range(0, g_dim):
            self.finger.append([(self.node_id + pow(2, i)) % pow(2, g_dim),
                                [(self.node_id + pow(2, i)) % pow(2, g_dim), (self.node_id + pow(2, i + 1)) % pow(2, g_dim)],
                                0])
        print self.finger

        findSuccessorMsg = message(self.node_id, 0, "find_successor", [self.finger[0][0]], "find_successor_ack")
        successor = findSuccessorMsg.send_and_get_response()[0]
        self.finger[0][2] = successor

        getPredecessorMsg = message(self.node_id, self.finger[0][2], "get_predecessor", ackID="get_predecessor_ack")
        predecessor = getPredecessorMsg.send_and_get_response()[0]
        self.predecessor = predecessor

        if self.node_id != self.finger[0][2]:
            setPredecessorMsg = message(self.node_id, self.finger[0][2], "set_predecessor", [self.node_id])
            setPredecessorMsg.send()
        else:
            self.predecessor = self.node_id

        for i in range(0, g_dim - 1):
            if self.finger[i + 1][0] in self.circularInterval(self.node_id, self.finger[i][2], True, False):
                self.finger[i + 1][2] = self.finger[i][2]
            else:
                findSuccessorMsg.msg_args = [self.finger[i + 1][0]]
                successor = findSuccessorMsg.send_and_get_response()[0]
                if successor not in self.circularInterval(self.finger[i + 1][0], self.node_id, True, True):
                    self.finger[i + 1][2] = self.node_id
                else:
                    self.finger[i + 1][2] = findSuccessorMsg.send_and_get_response()[0]

        print self.finger[0][2], self.predecessor, self.finger

    def update_others(self):
        for i in range(0, g_dim):
            p = self.find_predecessor((self.node_id - pow(2, i) + 1) % pow(2, g_dim))
            if p != self.node_id:
                updateFingerMsg = message(self.node_id, p, "update_finger_table", [self.node_id, i])
                updateFingerMsg.send()

    def update_finger_table(self, s, i):
        if s in self.circularInterval(self.node_id, self.finger[i][2], True, False):
            self.finger[i][2] = s
            p = self.predecessor
            if self.node_id != p and s != p:
                updateFingerMsg = message(self.node_id, p, "update_finger_table", [s, i])
                updateFingerMsg.send()

    def find_successor(self, id):
        print str(self.node_id) + " find_successor " + str(id)
        n_p = self.find_predecessor(id)
        if self.node_id != n_p:
            getSuccessorMsg = message(self.node_id, n_p, "get_successor", ackID="get_successor_ack")
            n_p_successor = getSuccessorMsg.send_and_get_response()[0]
        else:
            n_p_successor = self.finger[0][2]
        return n_p_successor

    def find_predecessor(self, id):
        n_p = self.node_id
        n_p_successor = self.finger[0][2]

        while id not in self.circularInterval(n_p, n_p_successor, False, True):
            getCPFMsg = message(self.node_id, n_p, "get_CPF", [id], "get_CPF_ack")
            getSuccessorMsg = message(self.node_id, n_p, "get_successor", ackID="get_successor_ack")
            if self.node_id != n_p:
                n_p = getCPFMsg.send_and_get_response()[0]
                n_p_successor = getSuccessorMsg.send_and_get_response()[0]
            else:
                n_p = self.closest_preceding_finger(id)
                getSuccessorMsg.to_id = n_p
                n_p_successor = getSuccessorMsg.send_and_get_response()[0]
        print str(self.node_id) + " find_predecessor " + str(id) + " = " + str(n_p)
        return n_p

    def closest_preceding_finger(self, id):
        for i in range(g_dim - 1, -1, -1):
            if self.finger[i][2] in self.circularInterval(self.node_id, id, False, False):
                return self.finger[i][2]
                print str(self.node_id) + " closest_preceding_finger " + str(id) + " = " + str(self.finger[i][2])

        print str(self.node_id) + " closest_preceding_finger " + str(id) + " = " + str(self.node_id)
        return self.node_id

    def circularInterval(self, start, end, startInclusive, endInclusive):
        if start >= end:
            interval = range(start + 1, pow(2, g_dim)) + range(0, end)
        else:
            interval = range(start + 1, end)
        if startInclusive:
            interval.append(start)
        if endInclusive:
            interval.append(end)
        return interval
    # parse the queue and call respective functions
    def parse_msg_queue(self):

        if not g_msg[self.node_id].empty():
            msg_tp = g_msg[self.node_id].get()

            from_id = msg_tp[0]
            msg = msg_tp[1]
            msg_args = msg_tp[2]
            print self.node_id, msg_tp
            if msg == 'get_successor':
                response = message(self.node_id, from_id, "get_successor_ack", [self.finger[0][2]])
                response.send()
            elif msg == 'get_CPF':
                response = message(self.node_id, from_id, "get_CPF_ack", [self.closest_preceding_finger(msg_args[0])])
                response.send()
            elif msg == 'find_successor':
                response = message(self.node_id, from_id, "find_successor_ack", [self.find_successor(msg_args[0])])
                response.send()
            elif msg == 'get_predecessor':
                response = message(self.node_id, from_id, "get_predecessor_ack", [self.predecessor])
                response.send()
            elif msg == 'set_predecessor':
                self.predecessor = msg_args[0]
            elif msg == 'update_finger_table':
                self.update_finger_table(msg_args[0], msg_args[1])

            elif msg == 'find':
                pass
                # double check if the msg is correctly pushed in the right queue
                # if int(tp[1]) != self.node_id:
                #     print 'message {0} pushed to wrong queue {1} \n'.format(cmd_msg, self.node_id)
                # else:
                #     key_id = int(tp[2])
                #     self.find_successor(cmd_msg, key_id)
            elif msg == 'leave':
                # leave the ring
                pass
            elif msg == 'show':
                # show the keys that it saved
                print 'Node {0} stores keys {1} \n'.format(self.node_id, self.keys)

    # # find successor
    # def find_successor(self, cmd_msg, key_id):
    #     if self.node_id < key_id <= self.successor:
    #         # found
    #         print 'Key {0} saved on node {1} \n'.format(key_id, self.successor)
    #     else:
    #         closest_node_id = self.closest_preceding_node(key_id)
    #         # push to the queue of the node
    #         g_msg[closest_node_id].put((cmd_msg, ''))


    # # find the closest preceding nodes in this table for id
    # def closest_preceding_node(self, key_id):
    #     cnt = g_dim
    #     while cnt >= 0:
    #         succ_node = self.finger[cnt][2]

    #         # may go around the circle
    #         if id <= self.node_id:
    #             tmp_id = key_id + math.pow(2, g_dim)
    #             tmp_succ_node = succ_node + math.pow(2, g_dim)
    #         else:
    #             tmp_id = key_id
    #             tmp_succ_node = succ_node

    #         # compare and find the closest node id
    #         if self.node_id < tmp_succ_node < tmp_id:
    #             return succ_node


# command line interface thread
class CmdThread(threading.Thread):
    def __init__(self, name):
        threading.Thread.__init__(self)
        self.name = name
        # create command line interface object
        self.cli = MP2Shell()

    def run(self):
        self.cli.cmdloop()
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

        # error check user input
        tp = arg.split()
        if len(tp) < 1:
            print("not enough parameters")
            return

        try:
            node_id = int(tp[0])
        except ValueError:
            print("node ID must be an integer between 0 and 255")
            return

        if node_id < 0 or node_id > 255:
            print("node ID must be an integer between 0 and 255")
            return

        # check if node exits
        if node_id not in n_threads.keys():
            n_threads[node_id] = NodeThread(node_id)
            #print 'created node n_{0}\n'.format(node_id)

            # start thread and join node to network
            n_threads[node_id].start()
            n_threads[node_id].join()

    def do_info(self, arg):
        tp = arg.split()
        print "predecessor: {0}, successor: {1}".format(n_threads[int(tp[0])].predecessor, n_threads[int(tp[0])].finger[0][2])
        print n_threads[int(tp[0])].finger

    def do_nodes(self, arg):
        x = n_threads.keys()
        x.sort()
        print x

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

    # start MP2Shell
    # MP2shell as the coordinator, creates and removes nodes.
    shell_thread = CmdThread('MP2Shell')
    print 'created shell thread'
    shell_thread.start()

    # initially join node 0 to the network
    shell_thread.cli.do_join('0')


if __name__ == '__main__':
    main(sys.argv[1:])