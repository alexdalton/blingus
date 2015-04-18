__author__ = 'Yanning Li and Alex Dalton'
# This code implements the chord protocol

from optparse import OptionParser
import socket
import sys
import threading
import time
import cmd
import Queue
import csv
import pprint
import random
import json
import numpy
import math


debug = False
coordQueue = Queue.Queue()

# use global queues for message passing between threads
# g_msg[node_id] = q_msg
# q_msg:
# ('shell input message', 'nodes message')
g_msg = {}
g_msg_cnt = 0

# global variable for the dimension
g_dim = 8

# global dictionary to store thread handles indexed by node ID
n_threads = {}

# optional file to write contents of show command to
outFile = None

# evaluation result
# g_eval[P] = (avg_add, avg_find)
g_eval = {}


class message():
    def __init__(self, from_id, to_id, msg, msg_args=[], ackID=None):
        self.from_id = from_id
        self.to_id = to_id
        self.msg = msg
        self.msg_args = msg_args
        self.ackID = ackID

    def send(self):
        global g_msg_cnt
        if self.to_id != self.from_id:
            g_msg_cnt += 1

        # sends a message to a node does not wait for a response
        if debug:
            print "send " + str([self.from_id, self.to_id, self.msg, self.msg_args])
        g_msg[self.to_id].put([self.from_id, self.msg, self.msg_args])



    def send_and_get_response(self):
        global g_msg_cnt
        if self.to_id != self.from_id:
            g_msg_cnt += 1

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
        self.finger = list()

        # successor(finger[1]); initialized as 0
        self.successor = 0
        self.predecessor = 0

        # save all the keys in a list of integers
        self.keys = []

        self.is_alive = True

        g_msg[node_id] = Queue.Queue()

    def run(self):
        # print "created node {0}".format(self.node_id)

        while self.is_alive:
            self.parse_msg_queue()
            #time.sleep(0.01)

        return 0

    def kill(self):
        self.is_alive = False

    def join(self):
        if self.node_id != 0:
            # node is not 0 (the first node) initialize finger table, update other nodes, and get keys that should be here
            self.init_finger_table()
            self.update_others()

            # transfer part of the keys to this node
            getKeysMsg = message(self.node_id,
                                 self.finger[0][2], "get_keys",
                                 ackID="get_keys_ack")
            self.keys = getKeysMsg.send_and_get_response()
            # transfer_keys_join_msg = message(self.node_id, self.successor,
            #                                  'transfer_keys_join', [])
            # transfer_keys_join_msg.send()

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
            if debug:
                print '   NODE {0} is leaving'.format(self.node_id)
                print '   NODE {0}: pred:{1}, succ:{2}'.format(self.node_id,
                                                               self.predecessor,
                                                               n_threads[self.node_id].finger[0][2])

            set_pred_succ_msg = message(self.node_id, self.predecessor,
                                        'set_successor', [self.successor])
            set_pred_succ_msg.send()

            # change successor's predecessor
            set_succ_pred_msg = message(self.node_id, self.successor,
                                        'set_predecessor', [self.predecessor])
            set_succ_pred_msg.send()

        # pass keys to successor
            transfer_key_msg = message(self.node_id, self.successor,
                                       'transfer_keys', self.keys)
            transfer_key_msg.send()

        # find all nodes that points to itself and ask them to update their pointer to
        # my successor
            self.update_others_leave()

    def init_finger_table(self):
        if debug:
            print ("Node: {0} function: init_finger_table".format(self.node_id))

        # initialize finger table with common values
        for i in range(0, g_dim):
            self.finger.append([(self.node_id + pow(2, i)) % pow(2, g_dim),
                                [(self.node_id + pow(2, i)) % pow(2, g_dim),
                                 (self.node_id + pow(2, i + 1)) % pow(2, g_dim)],
                                0])
        # print self.finger

        # get my successor from node 0
        findSuccessorMsg = message(self.node_id, 0, "find_successor",
                                   [self.finger[0][0]], "find_successor_ack")
        successor = findSuccessorMsg.send_and_get_response()[0]
        self.finger[0][2] = successor

        # get the predecessor from my successor (my predecessor now)
        getPredecessorMsg = message(self.node_id, self.finger[0][2],
                                    "get_predecessor", ackID="get_predecessor_ack")
        predecessor = getPredecessorMsg.send_and_get_response()[0]
        self.predecessor = predecessor

        # set the predecessor of my successor to be me
        if self.node_id != self.finger[0][2]:
            setPredecessorMsg = message(self.node_id, self.finger[0][2],
                                        "set_predecessor", [self.node_id])
            setPredecessorMsg.send()
        else:
            self.predecessor = self.node_id

        # update my other fingers
        for i in range(0, g_dim - 1):
            if self.finger[i + 1][0] in self.circularInterval(self.node_id,
                                                              self.finger[i][2],
                                                              True, False):
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

        # update successor
        self.successor = self.finger[0][2]
        if debug:
            print '    SET successor of node-{0} as: {1} from {2}'.format(self.node_id,
                                                                          self.successor,
                                                                          self.finger[0][2])

    def update_others(self):
        if debug:
            print ("Node: {0} function: update_others".format(self.node_id))
        for i in range(0, g_dim):
            p = self.find_predecessor((self.node_id - pow(2, i) + 1) % pow(2, g_dim))
            if p != self.node_id:
                updateFingerMsg = message(self.node_id, p,
                                          "update_finger_table",
                                          [self.node_id, i])
                updateFingerMsg.send()

    def update_finger_table(self, s, i):
        if debug:
            print("Node: {0} function: update_finger_table s = {1} i = {2}".format(self.node_id, s, i))

        if s in self.circularInterval(self.node_id,
                                      self.finger[i][2], True, False):
            self.finger[i][2] = s
            p = self.predecessor
            if self.node_id != p and s != p:
                updateFingerMsg = message(self.node_id, p,
                                          "update_finger_table", [s, i])
                updateFingerMsg.send()

            # update successor
            if i == 0:
                self.successor = self.finger[0][2]

    def find_successor(self, node_id):
        if debug:
            print("Node: {0} function: find_successor id = {1}".format(self.node_id, node_id))

        # print str(self.node_id) + " find_successor " + str(node_id)
        n_p = self.find_predecessor(node_id)
        if self.node_id != n_p:
            getSuccessorMsg = message(self.node_id, n_p, "get_successor", ackID="get_successor_ack")
            n_p_successor = getSuccessorMsg.send_and_get_response()[0]
        else:
            n_p_successor = self.finger[0][2]
        if debug:
            print("    return: {0}".format(n_p_successor))
        return n_p_successor

    def find_predecessor(self, node_id):
        if debug:
            print("Node: {0} function: find_predecessor id = {1}".format(self.node_id, node_id))

        n_p = self.node_id
        n_p_successor = self.finger[0][2]

        while node_id not in self.circularInterval(n_p, n_p_successor, False, True):
            getCPFMsg = message(self.node_id, n_p, "get_CPF", [node_id], "get_CPF_ack")
            getSuccessorMsg = message(self.node_id, n_p, "get_successor", ackID="get_successor_ack")
            if self.node_id != n_p:
                n_p = getCPFMsg.send_and_get_response()[0]
                getSuccessorMsg.to_id = n_p
                n_p_successor = getSuccessorMsg.send_and_get_response()[0]
            else:
                n_p = self.closest_preceding_finger(node_id)
                getSuccessorMsg.to_id = n_p
                n_p_successor = getSuccessorMsg.send_and_get_response()[0]
        if debug:
            print("    return: {0}".format(n_p))
        return n_p

    def closest_preceding_finger(self, node_id):
        if debug:
            print("Node: {0} function: closest_preceding_finger id = {1}".format(self.node_id, node_id))

        for i in range(g_dim - 1, -1, -1):
            if self.finger[i][2] in self.circularInterval(self.node_id, node_id, False, False):
                if debug:
                    print("    return: {0}".format(self.finger[i][2]))
                return self.finger[i][2]
                # print str(self.node_id) + " closest_preceding_finger " + str(node_id) + " = " + str(self.finger[i][2])

        # print str(self.node_id) + " closest_preceding_finger " + str(node_id) + " = " + str(self.node_id)
        if debug:
            print("    return: {0}".format(self.node_id))
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

    def update_others_leave(self):
        for i in range(0, g_dim):
            p = self.find_predecessor((self.node_id - pow(2, i) + 1) % pow(2, g_dim))
            # if not my self
            if p != self.node_id:
                updateFingerMsg = message(self.node_id, p,
                                          "update_finger_table_leave",
                                          [self.node_id, i, self.successor])
                updateFingerMsg.send()

    # s is the node to leave, s -> s_succ
    def update_finger_table_leave(self, s, i, s_succ):
        # if s in self.circularInterval(self.node_id, self.finger[i][2], True, False):
        if self.finger[i][2] == s:
            self.finger[i][2] = s_succ
            p = self.predecessor

            # update succcessor
            if i == 0:
                self.successor = self.finger[0][2]

            if self.node_id != p and s != p:
                updateFingerMsg = message(self.node_id, p,
                                          "update_finger_table_leave",
                                          [s, i, s_succ])
                updateFingerMsg.send()



    def parse_msg_queue(self):

        if not g_msg[self.node_id].empty():
            msg_tp = g_msg[self.node_id].get()

            from_id = msg_tp[0]
            msg = msg_tp[1]
            msg_args = msg_tp[2]

            if debug:
                print("received: {0}".format(msg_tp))

            # print self.node_id, msg_tp
            if msg == 'get_successor':
                response = message(self.node_id, from_id, "get_successor_ack", [self.finger[0][2]])
                response.send()
            elif msg == 'get_CPF':
                response = message(self.node_id, from_id, "get_CPF_ack",
                                   [self.closest_preceding_finger(msg_args[0])])
                response.send()
            elif msg == 'find_successor':
                if debug:
                    print 'find_successor:{0}'.format(msg_args)
                response = message(self.node_id, from_id, "find_successor_ack",
                                   [self.find_successor(msg_args[0])])
                response.send()
            elif msg == 'get_predecessor':
                response = message(self.node_id, from_id, "get_predecessor_ack",
                                   [self.predecessor])
                response.send()
            elif msg == 'set_predecessor':
                if debug:
                    print '\n set predecessor msg_args: {0}'.format(msg_args)
                    print '\n set_predecessor: {0}\n'.format(msg_args[0])
                self.predecessor = msg_args[0]
            elif msg == 'set_successor':
                if debug:
                    print '\n set successor msg_args: {0}'.format(msg_args)
                    print '\n set_successor: {0}\n '.format(msg_args[0])
                self.successor = msg_args[0]
                self.finger[0][2] = msg_args[0]
            elif msg == 'update_finger_table':
                self.update_finger_table(msg_args[0], msg_args[1])
            # this one is for transfer keys for join
            elif msg == 'get_keys':
                # transfer keys from this node to the requestor
                self.transfer_keys(from_id)
            # this is for transfer keys for leave
            elif msg == 'transfer_keys':
                # print 'transfer_keys_msg: {0}'.format(msg_tp)
                # print 'transfer_keys {0}'.format(msg_args)
                self.keys.extend(msg_args)
            elif msg == 'update_finger_table_leave':
                self.update_finger_table_leave(msg_args[0], msg_args[1], msg_args[2])
            # elif msg == 'show_keys':
            #     self.keys.sort()
            #     print 'node {0}: {1}'.format(self.node_id, self.keys)
            #
            #     # write to file
            #     if g_save_file is True:
            #         tmp_keys = self.keys
            #         tmp_keys.sort()
            #         tmp_keys.insert(0, self.node_id)
            #
            #         g_csv_handle.writerow(tmp_keys)

            # elif msg == 'transfer_keys_join':
            #     # transfer keys_id <= self.predecessor to self.predecessor
            #     keys_to_transfer = []
            #     # print 'self.keys: {0}'.format(self.keys)
            #     for key in self.keys:
            #         # print 'keys in self.key {0}'.format(key)
            #         if key in self.circularInterval(self.node_id, self.predecessor,
            #                                         False, True):
            #             # print 'keys_to_new_join {0}'.format(key)
            #             keys_to_transfer.append(key)
            #
            #     # remove those keys
            #     self.keys = [i for i in self.keys if i not in keys_to_transfer]
            #     transfer_keys_msg = message(self.node_id, self.predecessor,
            #                                 'transfer_keys', keys_to_transfer)
            #     # print 'keys to transfer {0}'.format(keys_to_transfer)
            #     transfer_keys_msg.send()
            elif msg == 'find':
                print(self.find_successor(msg_args[0]))
                coordQueue.put('find_ack')
                # double check if the msg is correctly pushed in the right queue
                # if int(tp[1]) != self.node_id:
                #     print 'message {0} pushed to wrong queue {1} \n'.format(cmd_msg, self.node_id)
                # else:
                #     key_id = int(tp[2])
                #     self.find_successor(cmd_msg, key_id)
            elif msg == 'join':
                self.join()
                coordQueue.put('join_ack')
            elif msg == 'leave':
                # leave the ring
                self.leave()
                coordQueue.put('leave_ack')
            elif msg == 'show':
                keyStr = ""
                for key in self.keys:
                    keyStr = keyStr + "{0} ".format(key)

                print "{0} {1}".format(self.node_id, keyStr)
                if outFile is not None:
                    fd = open(outFile, "a")
                    fd.write("{0} {1}\n".format(self.node_id, keyStr))
                    fd.close()
                coordQueue.put('show_ack')
                # show the keys that it saved
                # print 'Node {0} stores keys {1} \n'.format(self.node_id, self.keys)




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
            message(0, node_id, "join").send()
            coordQueue.get(block=True)

    def do_info(self, arg):
        """
        Show info of node : info 2
        predecessor successor, fingers
        :param arg:
        :return:
        """
        tp = arg.split()
        if int(tp[0]) in n_threads.keys():
            print "predecessor: {0}, successor: {1}".format(n_threads[int(tp[0])].predecessor,
                                                        n_threads[int(tp[0])].finger[0][2])
            print '{0}.predecessor:{1}, {2}.successor:{3}'.format(int(tp[0]),
                                                                  n_threads[int(tp[0])].predecessor,
                                                                  int(tp[0]),
                                                                  n_threads[int(tp[0])].successor)
            print("finger table:")
            pprint.pprint(n_threads[int(tp[0])].finger)
            print("keys:")
            n_threads[int(tp[0])].keys.sort()
            print(n_threads[int(tp[0])].keys)
        else:
            print 'Node {0} does not exist'.format(tp[0])

    def do_nodes(self, arg):
        """
        The nodes in the ring
        :param arg:
        :return:
        """
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
        if node_id not in n_threads.keys():
            print 'Error, node {0} does not exist \n'.format(node_id)
        else:
            key_id = int(tp[1])
            find_key_msg = message(node_id, node_id,
                                   'find', [key_id])
            find_key_msg.send()
            ack = coordQueue.get(block=True)


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
            message(0, node_id, "leave").send()
            coordQueue.get(block=True)
            # n_threads[node_id].leave()
            # g_msg[node_id].put((arg, ''))
            # end the thread
            n_threads[node_id].is_alive = False

            # not completely sure if the thread is correctly closed
            n_threads.pop(node_id, None)

            # remove the message queue
            g_msg.pop(node_id, None)

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
            node_id = int(tp[0])
            if node_id not in g_msg.keys():
                print 'Error, node {0} does not exist \n'.format(node_id)
            else:
                show_msg = message(node_id, node_id, 'show', [])
                show_msg.send()
                coordQueue.get(block=True)
                # g_msg[node_id].put((arg, ''))

    def do_bye(self, arg):
        """
        :param arg: bye (stop code)
        :return: true
        """
        print 'stop demo'
        return True
        # sys.exit(0)

    def do_evaluate(self, arg):
        """
        evaluate the performance
        :param arg: N
        :return:
        """

        if len(arg) != 1:
            print 'Not enough argument, input N'
            return True

        # number of rounds to average
        N = int(arg[0])
        # experiment scenarios
        P_list = list([4, 8, 10, 20, 30])
        # P_list = list([4, 8, 10])
        # P_list = list([4])
        F = 64

        for P in P_list:

            print 'In Senario P = {0}, F = 64'.format(P)
            # a list for messages
            msg_cnt_add = list()
            msg_cnt_find = list()

            # run N times
            for n in range(1, N+1):

                print 'P={0}, N={1}'.format(P, n)
                # create a scenario
                scenario = singleScenario(P, F)

                scenario.add_all_nodes()
                # count the messages for adding nodes
                msg_cnt_add.append(scenario.get_msg_cnt_add())

                print 'finished added nodes'

                # randomly generate p and k
                scenario.find_F_keys()
                # count the messages for finding
                msg_cnt_find.append(scenario.get_msg_cnt_find())

                # remove all nodes in this scenario
                scenario.remove_all_nodes()

            # output the average for this scenario
            print 'average add in P = {0}, F = 64 is :{1}'.format(
                P, sum(msg_cnt_add)/float(N*P))
            print 'average find in P = {0}, F = 64 is :{1}'.format(
                P, sum(msg_cnt_find)/float(N*F))

            # save to global variable
            g_eval[P] = (sum(msg_cnt_add)/float(N*P),
                         sum(msg_cnt_find)/float(N*F))

    def do_result(self, arg):
        """
        show evaluation result
        :param arg:
        :return:
        """
        print 'Evaluation results: (add, find)'
        x = g_eval.keys()
        x.sort()
        for p in x:
            print '    P={0}, F=64:{1}'.format(p, g_eval[p])


    def emptyline(self):
        # overwrite to not execute last command when hit enter
        pass


class singleScenario():
    def __init__(self, num_nodes, num_finds):
        self.msg_cnt_add = 0
        self.msg_cnt_find = 0
        self.num_nodes = num_nodes
        self.num_finds = num_finds

    # randomly generate and add ndoes to chord
    def add_all_nodes(self):
        # regenerate seed
        random.seed()

        nodes = random.sample(range(0, pow(2, g_dim)), self.num_nodes)

        print 'Adding all nodes:{0}'.format(nodes)

        if debug:
            print 'Generated nodes: {0}'.format(nodes)
        for node_id in nodes:
            if node_id not in n_threads.keys():

                print 'Adding node {0}'.format(node_id)

                n_threads[node_id] = NodeThread(node_id)
                #print 'created node n_{0}\n'.format(node_id)

                # start thread and join node to network
                n_threads[node_id].start()
                message(0, node_id, "join").send()
                coordQueue.get(block=True)

        print 'checking adding nodes:{0}'.format(n_threads.keys())

    # conduct F finds from randomly selected nodes
    # and randomly generated keys
    def find_F_keys(self):
        # generate F unique keys once for all
        random.seed()

        keys = random.sample(range(0, pow(2, g_dim)), self.num_finds)
        print 'randomly generated keys: {0}'.format(keys)
        # randomly select one node each time
        for i in range(0, self.num_finds):
            node_id = random.choice(n_threads.keys())

            print 'to find key {0} from node {1}'.format(keys[i], node_id)

            find_key_msg = message(node_id, node_id, 'find',
                    [keys[i]])
            find_key_msg.send()
            ack = coordQueue.get(block=True)

    # get the msgs in add
    def get_msg_cnt_add(self):
        global g_msg_cnt

        self.msg_cnt_add = g_msg_cnt
        # reset count
        g_msg_cnt = 0
        return self.msg_cnt_add

    # assume always first add, then find
    def get_msg_cnt_find(self):
        global g_msg_cnt
        self.msg_cnt_find = g_msg_cnt
        # reset count
        g_msg_cnt = 0
        return self.msg_cnt_find

    # remvove all nodes in this scenario
    def remove_all_nodes(self):

        print 'Removing all nodes:{0}'.format(n_threads.keys())

        for node_id in n_threads.keys():
            if node_id != 0:

                print 'Leaving node {0}'.format(node_id)

                remove_msg = message(node_id, node_id, "leave")
                remove_msg.send()
                coordQueue.get(block=True)

                # give enough time for leaving
                time.sleep(0.3)

                n_threads[node_id].is_alive = False

                # for thread to end properly
                time.sleep(0.1)

                n_threads.pop(node_id, None)

                g_msg.pop(node_id, None)

                print 'node {0} left.'.format(node_id)
                print 'node to leave.{0}'.format(n_threads.keys())

        # check if properly removed
        if len(n_threads.keys()) == 1:
            for node_id in n_threads.keys():
                print 'ALL nodes removed except {0}'.format(node_id)
                print "predecessor: {0}, successor: {1}".format(n_threads[node_id].predecessor,
                                                                n_threads[node_id].finger[0][2])
                print("finger table:")
                pprint.pprint(n_threads[node_id].finger)
                print("keys:")
                n_threads[node_id].keys.sort()
                print(n_threads[node_id].keys)





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

    main(sys.argv[1:])