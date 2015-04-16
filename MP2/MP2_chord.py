__author__ = 'Yanning Li and Alex Dalton'
# This code implements the chord protocol


import socket
import sys
import threading
import time
import cmd
import Queue
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
        self.finger = list()

        # successor(finger[1]); initialized as 0
        self.successor = 0
        self.predecessor = 0

        # save all the keys in a list of integers
        self.keys = []

        self.is_alive = True

        g_msg[node_id] = Queue.Queue()

    def run(self):
        print "created node {0}".format(self.node_id)

        while self.is_alive:
            self.parse_msg_queue()
            time.sleep(0.1)

    def kill(self):
        self.is_alive = False

    def join(self):
        if self.node_id != 0:
            self.init_finger_table()
            self.update_others()

            # transfer part of the keys to this node
        else:
            self.keys = range(0, pow(2, g_dim))
            for i in range(0, g_dim):
                self.finger.append([(self.node_id + pow(2, i)) % pow(2, g_dim),
                                    [(self.node_id + pow(2, i)) % pow(2, g_dim),
                                     (self.node_id + pow(2, i + 1)) % pow(2, g_dim)],
                                    0])

    def leave(self):

        if self.node_id == 0:
            print 'Error: Node 0 can not leave\n'
        else:

            # ask its predecessor to change successor to next node
            set_pred_succ_msg = message(self.node_id, self.predecessor,
                                        'set_successor', self.successor,
                                        'set_successor_ack')
            set_pred_succ_msg.send()

            # change successor's predecessor
            set_succ_pred_msg = message(self.node_id, self.successor,
                                        'set_predecessor', self.predecessor,
                                        'set_predecessor_ack')
            set_succ_pred_msg.send()

        # pass keys to successor
            transfer_key_msg = message(self.node_id, self.successor,
                                       'transfer_keys', self.keys,
                                       'transfer_keys_ack')
            transfer_key_msg.send()

        # find all nodes that points to itself and ask them to update their pointer to
        # my successor
            self.update_others_leave()

    def init_finger_table(self):
        for i in range(0, g_dim):
            self.finger.append([(self.node_id + pow(2, i)) % pow(2, g_dim),
                                [(self.node_id + pow(2, i)) % pow(2, g_dim),
                                 (self.node_id + pow(2, i + 1)) % pow(2, g_dim)],
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

    def find_successor(self, node_id):
        print str(self.node_id) + " find_successor " + str(node_id)
        n_p = self.find_predecessor(node_id)
        if self.node_id != n_p:
            getSuccessorMsg = message(self.node_id, n_p, "get_successor", ackID="get_successor_ack")
            n_p_successor = getSuccessorMsg.send_and_get_response()[0]
        else:
            n_p_successor = self.finger[0][2]
        return n_p_successor

    def find_predecessor(self, node_id):
        n_p = self.node_id
        n_p_successor = self.finger[0][2]

        while node_id not in self.circularInterval(n_p, n_p_successor, False, True):
            getCPFMsg = message(self.node_id, n_p, "get_CPF", [node_id], "get_CPF_ack")
            getSuccessorMsg = message(self.node_id, n_p, "get_successor", ackID="get_successor_ack")
            if self.node_id != n_p:
                n_p = getCPFMsg.send_and_get_response()[0]
                n_p_successor = getSuccessorMsg.send_and_get_response()[0]
            else:
                n_p = self.closest_preceding_finger(node_id)
                getSuccessorMsg.to_id = n_p
                n_p_successor = getSuccessorMsg.send_and_get_response()[0]
        print str(self.node_id) + " find_predecessor " + str(node_id) + " = " + str(n_p)
        return n_p

    def closest_preceding_finger(self, node_id):
        for i in range(g_dim - 1, -1, -1):
            if self.finger[i][2] in self.circularInterval(self.node_id, node_id, False, False):
                return self.finger[i][2]
                print str(self.node_id) + " closest_preceding_finger " + str(node_id) + " = " + str(self.finger[i][2])

        print str(self.node_id) + " closest_preceding_finger " + str(node_id) + " = " + str(self.node_id)
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

    def update_others_leave(self):
        for i in range(0, g_dim):
            p = self.find_predecessor((self.node_id - pow(2, i) + 1) % pow(2, g_dim))
            if p != self.node_id:
                updateFingerMsg = message(self.node_id, p,
                                          "update_finger_table_leave",
                                          [self.node_id, i, self.successor])
                updateFingerMsg.send()

    def update_finger_table_leave(self, s, i, s_succ):
        if s in self.circularInterval(self.node_id, self.finger[i][2], True, False):
            self.finger[i][2] = s_succ
            p = self.predecessor
            if self.node_id != p and s != p:
                updateFingerMsg = message(self.node_id, p,
                                          "update_finger_table",
                                          [s, i, s_succ])
                updateFingerMsg.send()

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
            elif msg == 'set_successor':
                self.successor = msg_args[0]
                self.finger[0][2] = msg_args[0]
            elif msg == 'update_finger_table':
                self.update_finger_table(msg_args[0], msg_args[1])
            elif msg == 'transfer_keys':
                self.keys.extend(msg_args[0])
            elif msg == 'update_finger_table_leave':
                self.update_finger_table(msg_args[0], msg_args[1], msg_args[2])
            elif msg == 'show_keys':
                print 'node {0}: {1}'.format(self.node_id, self.keys)
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


    #
    # # parse the queue and call respective functions
    # def parse_msg_queue(self):
    #
    #     if not g_msg[self.node_id].empty():
    #         msg_tp = g_msg[self.node_id].get()
    #
    #         cmd_msg = msg_tp[0]
    #         tp = cmd_msg.split()
    #
    #         if tp[0] == 'join':
    #             # join it self to the ring
    #             pass
    #         elif tp[0] == 'find':
    #             # double check if the msg is correctly pushed in the right queue
    #             if int(tp[1]) != self.node_id:
    #                 print 'message {0} pushed to wrong queue {1} \n'.format(cmd_msg, self.node_id)
    #             else:
    #                 key_id = int(tp[2])
    #                 self.find_successor(cmd_msg, key_id)
    #         elif tp[0] == 'leave':
    #             # leave the ring
    #             node_id = tp[1]
    #
    #             # if is itself
    #             if node_id == self.node_id:
    #                 # ask its predecessor to change successor to next node
    #                 g_msg[self.predecessor].put((cmd_msg, 'successor:{0}'.format(self.successor)))
    #
    #                 # pass keys to successor and change successor's predecessor
    #                 g_msg[self.successor].put((cmd_msg, 'keys:{0}'.format(self.keys)))
    #                 g_msg[self.successor].put((cmd_msg, 'predecessor:{0}'.format(self.predecessor)))
    #
    #                 # find all nodes that points to itself and ask them to update.
    #
    #             # asked by the node who is leaving
    #             else:
    #                 data_msg = msg_tp[1]
    #                 data_tp = data_msg.split(':')
    #
    #                 if data_tp[0] == 'successor':
    #                     self.successor = data_tp[1]
    #                 elif data_tp[0] == 'predecessor':
    #                     self.predecessor = data_tp[1]
    #                 elif data_tp[0] == 'keys':
    #                     pass
    #
    #
    #         elif tp[0] == 'show':
    #             # show the keys that it saved
    #             print 'Node {0} stores keys {1} \n'.format(self.node_id, self.keys)
    #



# command line interface thread
class CmdThread(threading.Thread):
    def __init__(self, name):
        threading.Thread.__init__(self)
        # self.threadID = thread_id
        self.name = name
        self.cli = MP2Shell()

    def run(self):

        # print 'created cmd line interface object'
        self.cli.cmdloop()
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
        node_id = int(tp[0])
        if node_id not in g_msg.keys():
            print 'Error, node {0} does not exist \n'.format(node_id)
        else:
            key_id = int(tp[1])
            find_key_msg = message(node_id, node_id, 'find_successor', key_id)
            find_key_msg.send()
            #g_msg[node_id].put((arg, ''))

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
        node_id = int(tp[0])
        if node_id not in g_msg.keys():
            print 'Error, node {0} does not exist \n'.format(node_id)
        else:
            n_threads[node_id].leave()
            # g_msg[node_id].put((arg, ''))

    def do_show(self, arg):
        """
        show keys stored in node p, or all
        :param arg: node id p, or 'all'
        :return:
        """
        print arg
        tp = arg.split()
        if len(tp) < 1:
            print("not enough parameters")
            return

        if tp[0] == 'all':
            # push to all queues
            # each node will print out one table showing its info
            for key in g_msg:
                show_msg = message(key, key, 'show_keys', [])
                show_msg.send()
                # g_msg[key].put((arg, ''))
        else:
            node_id = int(tp[0])
            if node_id not in g_msg.keys():
                print 'Error, node {0} does not exist \n'.format(node_id)
            else:
                show_msg = message(node_id, node_id, 'show_keys', [])
                show_msg.send()
                # g_msg[node_id].put((arg, ''))

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