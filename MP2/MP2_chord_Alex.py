__author__ = 'Yanning Li and Alex Dalton'
# This code implements the chord protocol

from optparse import OptionParser
import socket
import sys
import threading
import time
import cmd
import Queue
import json
import pprint
import math

debug = False

coordQueue = Queue.Queue()
# use global queues for message passing between threads
# g_msg[node_id] = q_msg
# q_msg:
# ('shell input message', 'nodes message')
g_msg = {}

# global variable for the dimension
g_dim = 8

# global dictionary to store thread handles indexed by node ID
n_threads = {}

# optional file to write contents of show command to
outFile = None

class message():
    # message class for sending messages between the nodes
    def __init__(self, from_id, to_id, msg, msg_args=[], ackID=None):
        self.from_id = from_id
        self.to_id = to_id
        self.msg = msg
        self.msg_args = msg_args
        self.ackID = ackID

    def send(self):
        # sends a message to a node does not wait for a response
        if debug:
            print "send " + str([self.from_id, self.to_id, self.msg, self.msg_args])
        g_msg[self.to_id].put([self.from_id, self.msg, self.msg_args])

    def send_and_get_response(self):
        # sends a message to a node and waits for a response
        if debug:
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
            #time.sleep(0.1)

    def kill(self):
        self.is_alive = False

    def join(self):
        if self.node_id != 0:
            # node is not 0 (the first node) initialize finger table, update other nodes, and get keys that should be here
            self.init_finger_table()
            self.update_others()
            getKeysMsg = message(self.node_id, self.finger[0][2], "get_keys", ackID="get_keys_ack")
            self.keys = getKeysMsg.send_and_get_response()
        else:
            # node is 0 (first node) initialize finger table and set all keys to be here
            self.keys = range(0, pow(2, g_dim))
            for i in range(0, g_dim):
                self.finger.append([(self.node_id + pow(2, i)) % pow(2, g_dim),
                                    [(self.node_id + pow(2, i)) % pow(2, g_dim), (self.node_id + pow(2, i + 1)) % pow(2, g_dim)],
                                    0])

    def init_finger_table(self):
        if debug:
            print ("Node: {0} function: init_finger_table".format(self.node_id))

        # initialize finger table with common values
        for i in range(0, g_dim):
            self.finger.append([(self.node_id + pow(2, i)) % pow(2, g_dim),
                                [(self.node_id + pow(2, i)) % pow(2, g_dim), (self.node_id + pow(2, i + 1)) % pow(2, g_dim)],
                                0])

        # get my successor from node 0
        findSuccessorMsg = message(self.node_id, 0, "find_successor", [self.finger[0][0]], "find_successor_ack")
        successor = findSuccessorMsg.send_and_get_response()[0]
        self.finger[0][2] = successor

        # get the predecessor from my successor (my predecessor now)
        getPredecessorMsg = message(self.node_id, self.finger[0][2], "get_predecessor", ackID="get_predecessor_ack")
        predecessor = getPredecessorMsg.send_and_get_response()[0]
        self.predecessor = predecessor

        # set the predecessor of my successor to be me
        if self.node_id != self.finger[0][2]:
            setPredecessorMsg = message(self.node_id, self.finger[0][2], "set_predecessor", [self.node_id])
            setPredecessorMsg.send()
        else:
            self.predecessor = self.node_id

        # update my other fingers
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
        if debug:
            print("    successor: {0}\n    predecessor: {1}\n    finger table: {2}".format(self.finger[0][2], self.predecessor, self.finger))

    def update_others(self):
        if debug:
            print ("Node: {0} function: update_others".format(self.node_id))
        for i in range(0, g_dim):
            p = self.find_predecessor((self.node_id - pow(2, i) + 1) % pow(2, g_dim))
            if p != self.node_id:
                updateFingerMsg = message(self.node_id, p, "update_finger_table", [self.node_id, i])
                updateFingerMsg.send()

    def update_finger_table(self, s, i):
        if debug:
            print("Node: {0} function: update_finger_table s = {1} i = {2}".format(self.node_id, s, i))
        if s in self.circularInterval(self.node_id, self.finger[i][2], True, False):
            self.finger[i][2] = s
            p = self.predecessor
            if self.node_id != p and s != p:
                updateFingerMsg = message(self.node_id, p, "update_finger_table", [s, i])
                updateFingerMsg.send()

    def find_successor(self, id):
        if debug:
            print("Node: {0} function: find_successor id = {1}".format(self.node_id, id))
        n_p = self.find_predecessor(id)
        if self.node_id != n_p:
            getSuccessorMsg = message(self.node_id, n_p, "get_successor", ackID="get_successor_ack")
            n_p_successor = getSuccessorMsg.send_and_get_response()[0]
        else:
            n_p_successor = self.finger[0][2]
        if debug:
            print("    return: {0}".format(n_p_successor))
        return n_p_successor

    def find_predecessor(self, id):
        if debug:
            print("Node: {0} function: find_predecessor id = {1}".format(self.node_id, id))
        n_p = self.node_id
        n_p_successor = self.finger[0][2]

        while id not in self.circularInterval(n_p, n_p_successor, False, True):
            getCPFMsg = message(self.node_id, n_p, "get_CPF", [id], "get_CPF_ack")
            getSuccessorMsg = message(self.node_id, n_p, "get_successor", ackID="get_successor_ack")
            if self.node_id != n_p:
                n_p = getCPFMsg.send_and_get_response()[0]
                getSuccessorMsg.to_id = n_p
                n_p_successor = getSuccessorMsg.send_and_get_response()[0]
            else:
                n_p = self.closest_preceding_finger(id)
                getSuccessorMsg.to_id = n_p
                n_p_successor = getSuccessorMsg.send_and_get_response()[0]
        if debug:
            print("    return: {0}".format(n_p))
        return n_p

    def closest_preceding_finger(self, id):
        if debug:
            print("Node: {0} function: closest_preceding_finger id = {1}".format(self.node_id, id))
        for i in range(g_dim - 1, -1, -1):
            if self.finger[i][2] in self.circularInterval(self.node_id, id, False, False):
                if debug:
                    print("    return: {0}".format(self.finger[i][2]))
                return self.finger[i][2]
        if debug:
            print("    return: {0}".format(self.node_id))
        return self.node_id

    def circularInterval(self, start, end, startInclusive, endInclusive):
        # returns a set of numbers that represent the circular interval between start and end
        # startInclusive and endInclusive for whether these endpoints should be included in the set or not
        if start >= end:
            interval = range(start + 1, pow(2, g_dim)) + range(0, end)
        else:
            interval = range(start + 1, end)
        if startInclusive:
            interval.append(start)
        if endInclusive:
            interval.append(end)
        return interval

    def transfer_keys(self, from_id):
        # transfer keys from this node to the from_id
        theirKeys = []

        # get list of keys that should be at from_id and remove from this self.keys
        for i in range(0, len(self.keys)):
            if self.keys[i] <= from_id and self.keys[i] != self.node_id:
                theirKeys.append(self.keys[i])
        for key in theirKeys:
            self.keys.remove(key)

        # send keys to from_id
        transferMsg = message(self.node_id, from_id, "get_keys_ack", theirKeys)
        transferMsg.send()

    # parse the queue and call respective functions
    def parse_msg_queue(self):

        if not g_msg[self.node_id].empty():
            msg_tp = g_msg[self.node_id].get()

            # get contents of message
            from_id = msg_tp[0]
            msg = msg_tp[1]
            msg_args = msg_tp[2]

            if debug:
                print("received: {0}".format(msg_tp))

            if msg == 'get_successor':
                # send response with this nodes successor
                response = message(self.node_id, from_id, "get_successor_ack", [self.finger[0][2]])
                response.send()
            elif msg == 'get_CPF':
                # send response for what this nodes closest preceding finger to argument
                response = message(self.node_id, from_id, "get_CPF_ack", [self.closest_preceding_finger(msg_args[0])])
                response.send()
            elif msg == 'find_successor':
                # send response for the successor of argument
                response = message(self.node_id, from_id, "find_successor_ack", [self.find_successor(msg_args[0])])
                response.send()
            elif msg == 'get_predecessor':
                # send response with this nodes predecessor
                response = message(self.node_id, from_id, "get_predecessor_ack", [self.predecessor])
                response.send()
            elif msg == 'set_predecessor':
                # set this nodes predecessor to be argument
                self.predecessor = msg_args[0]
            elif msg == 'update_finger_table':
                # update this nodes finger table with s, i
                self.update_finger_table(msg_args[0], msg_args[1])
            elif msg == 'get_keys':
                # transfer keys from this node to the requestor
                self.transfer_keys(from_id)
            elif msg =='join':
                # join this node to the network
                self.join()
                coordQueue.put('join_ack')
            elif msg == 'find':
                # find a key (same thing as find_successor)
                print(self.find_successor(msg_args[0]))
                coordQueue.put('find_ack')
            elif msg == 'leave':
                # leave the ring
                pass
            elif msg == 'show':
                # show the keys at this node with optional writing to file
                keyStr = ""
                for key in self.keys:
                    keyStr = keyStr + "{0} ".format(key)

                print "{0} {1}".format(self.node_id, keyStr)
                if outFile is not None:
                    fd = open(outFile, "a")
                    fd.write("{0} {1}\n".format(self.node_id, keyStr))
                    fd.close()
                coordQueue.put('show_ack')


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

            # start thread and join node to network
            n_threads[node_id].start()
            message(0, node_id, "join").send()
            coordQueue.get(block=True)

    def do_info(self, arg):
        # debug function to print out information on a given node
        tp = arg.split()
        print "predecessor: {0}\nsuccessor: {1}".format(n_threads[int(tp[0])].predecessor, n_threads[int(tp[0])].finger[0][2])
        print("finger table:")
        pprint.pprint(n_threads[int(tp[0])].finger)
        print("keys:")
        print(n_threads[int(tp[0])].keys)

    def do_nodes(self, arg):
        # debug function to just print out all the nodes currently running
        x = n_threads.keys()
        x.sort()
        print x

    def do_find(self, arg):
        """
        ask node p to locate key k
        :param arg: node id p; key id k
        :return:
        """
        # input checking
        tp = arg.split()
        if len(tp) < 2:
            print("not enough parameters \n")
            return

        # get p and k arguments
        p = int(tp[0])
        k = int(tp[1])

        # send find message to p with arg k if p exists
        if p in n_threads.keys():
            findMsg = message(0, p, "find", [k])
            findMsg.send()
            ack = coordQueue.get(block=True)

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

        # input checking
        tp = arg.split()
        if len(tp) < 1:
            print("not enough parameters")
            return

        # if all send show message to all nodes, wait for response before sending to next node
        if tp[0] == 'all':
            nodes = n_threads.keys()
            nodes.sort()
            for node_id in nodes:
                showMsg = message(0, node_id, "show")
                showMsg.send()
                coordQueue.get(block=True)
        # else send the message to just the one node
        else:
            showMsg = message(0, int(tp[0]), "show")
            showMsg.send()
            coordQueue.get(block=True)

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
    # get optional file name to write contents of show command to
    parser = OptionParser()
    parser.add_option("-g", "--file", dest="outFile", help="file to write contents of show command to", metavar="FILE")
    (options, args) = parser.parse_args()

    # clear the file if it exists and set global variable to file name
    if options.outFile:
        outFile = options.outFile
        open(outFile, "w").close()

    # begin chord simulation
    main(sys.argv[1:])