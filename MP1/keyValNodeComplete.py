__author__ = 'Yanning Li'
# this code implements a network node, which listens
# (as a server), sends messages (as a client),
# with delay function, and
# command line interface

import socket
import sys
import threading
import time
import cmd
import Queue
import csv
import random
import json

# define global queues
# to be pushed to channel (delay happens in channel)
# a tuple, destination(ABCD), and message
q_toChannel = Queue.Queue()

# to be send out (after delay)
# a tuple: destination (ABCD), message
q_toSend = Queue.Queue()

# key value store for this node
keyVal = {}
# received messages for this node
q_received = Queue.Queue()

# received messages for the repair thread
q_repair = Queue.Queue()

# received messages from a search operation
q_search = Queue.Queue()


# self node name and msg_cnt to define the unique msg_id
# msg_id = A12 g_node_name + g_msg_cnt
# msg_id is not the number sent by sequencer
g_node_name = ''
g_msg_cnt = 0

g_r_g = 0

# hold back queue of messages, implemented as a dict [msg_id] = msg
dict_holdback = {}

# the order of messages that should be delivered defined by sequencer
# [s_g] = msg_id
dict_order = {}

# delivered queue from TO Multi-cast
q_delivered = Queue.Queue()



# thread that fixes consistencies for models 3 and 4
class repairThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        while True:
            # wake up every 3 seconds to repair
            time.sleep(3)

            # send repair message to all nodes
            nodeKeyVals = {}
            sendString = "repair 3"
            q_toChannel.put(('A', sendString))
            q_toChannel.put(('B', sendString))
            q_toChannel.put(('C', sendString))
            q_toChannel.put(('D', sendString))

            # receive key-val stores from all 4 nodes and mark inconsistent keys
            inconsistents = {}
            for i in range(0, 4):
                items = q_repair.get(block=True).split('|')
                theirKeyVal = json.loads(items[1])
                for k, v in theirKeyVal.iteritems():
                    key = int(k)
                    value = int(v[0])
                    timestamp = float(v[1])
                    if key not in nodeKeyVals.keys():
                        nodeKeyVals[key] = []
                    nodeKeyVals[key].append((timestamp, value))
                    if len(nodeKeyVals[key]) > 1 and nodeKeyVals[key][-1] != nodeKeyVals[key][0]:
                        inconsistents[key] = 1

            # if node is missing a key mark that as inconsistent key
            for k, v in nodeKeyVals.iteritems():
                if len(v) < 4:
                    inconsistents[k] = 1

            # for all inconsistent keys send a write to all nodes with most recent value
            for key in inconsistents.keys():
                newest = max(nodeKeyVals[key])
                sendString = "repairWrite 3 {0} {1} {2}".format(key, newest[1], newest[0]) # make sure this only writes if the timestamp is >= what's already in keyVal
                q_toChannel.put(('A', sendString))
                q_toChannel.put(('B', sendString))
                q_toChannel.put(('C', sendString))
                q_toChannel.put(('D', sendString))


# the client thread which constantly read q_toSend and send out messages
class SendThread(threading.Thread):
    def __init__(self, node_dict, name, is_alive):
        threading.Thread.__init__(self)
        self.node_dict = node_dict
        self.node_name = name
        self.is_alive = is_alive

    def run(self):
        # print "Starting sending thread"

        while self.is_alive:
            if not q_toSend.empty():
                item = q_toSend.get()
                dest = self.node_dict[item[0]]

                UDP_IP = dest[0]
                UDP_PORT = int(dest[1])
                MESSAGE = self.node_name + ';' + item[1]

                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.sendto(MESSAGE, (UDP_IP, UDP_PORT))

            else:
                time.sleep(0.1)

    def kill(self):
            self.is_alive = False


# the server thread class which listens incoming messages
# create a thread to handle incoming messages once sock bind
class ReceiveThread(threading.Thread):
    def __init__(self, name, sock, node_dict, delay_dict, is_alive):
        threading.Thread.__init__(self)
        self.name = name
        self.sock = sock
        self.node_dict = node_dict
        self.delay_dict = delay_dict
        self.repairStarted = False
        self.is_alive = is_alive


    def run(self):
        global g_r_g
        global g_node_name
        # print 'node ' + self.name + ' starts listening:'

        while self.is_alive:
            msg_str, addr = self.sock.recvfrom(1024)
            msg = msg_str.split(';')

            sender = msg[0]
            data = msg[1]

            # Model 1 and 2:
            #   <sender; data; msg_id> from ABCD
            #   <sender; data; msg_id; s_g> from sequencer
            # For model 3 and 4
            #   <sender, data>
            if len(msg) == 3:
                msg_id = msg[2]
            elif len(msg) == 4:
                msg_id = msg[2]
                s_g = msg[3]


            # find out the maximal delay
            chn = sender + self.name
            if self.delay_dict[chn] is None:
                print 'invalid channel' + chn
            else:
                delay_max = int(self.delay_dict[chn])

            # print message
            # print 'Received "' + data + '" from ' + sender + ', Max delay is ' \
            #     + str(delay_max) + 's, system time is ' + \
            #     time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

            items = data.split()

            if len(items) == 1:
                # if it is just a send hello message
                print 'Received "' + data + '" from ' + sender + ', Max delay is ' \
                + str(delay_max) + 's, system time is ' + \
                time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

            else:
                # if delete just delete the key, only for model 3 and 4
                    # delete need to go through TO-MC for model 1 and 2
                    # <delete mdl key>
                if items[0] == 'delete':
                    key = int(items[1])
                    if key in keyVal.keys():
                        del keyVal[key]
                    else:
                        print("Can't delete key {0}, doesn't exist".format(key))
                    q_toChannel.put((sender, 'deleteAck {0}'.format(key)))
                    continue

                if items[0] == 'deleteAck':
                    q_received.put(data)
                    continue

                if items[0] == 'search':
                    key = int(items[1])
                    if key in keyVal.keys():
                        q_toChannel.put((sender, 'searchAck {0}'.format(self.name)))
                    else:
                        q_toChannel.put((sender, 'searchAck F'))
                    continue

                if items[0] == 'searchAck':
                    q_search.put(data)
                    continue

                # models 3 & 4 do stuff with received messages
                if int(items[1]) > 2:
                    # start the repair thread if not started(only need one)
                    if(self.name == 'A' and self.repairStarted == False):
                        repair_thread = repairThread()
                        repair_thread.start()
                        self.repairStarted = True

                    # if insert, insert into keyVal store and send sender an ack
                    if items[0] == 'insert':
                        key = int(items[2])
                        value = int(items[3])
                        timestamp = float(items[4])
                        if (key in keyVal.keys() and keyVal[key][1] <= timestamp) or (key not in keyVal.keys()):
                            keyVal[key] = (value, timestamp)
                        q_toChannel.put((sender, 'ack 3 {0}'.format(data)))

                    # if update try to update keyVal store and send sender an ack
                    elif items[0] == 'update':
                        key = int(items[2])
                        value = int(items[3])
                        timestamp = float(items[4])
                        if (key in keyVal.keys() and keyVal[key][1] <= timestamp):
                            keyVal[key] = (value, timestamp)
                        elif (key not in keyVal.keys()):
                            print("Can't update key {0}, doesn't exist".format(key))
                        q_toChannel.put((sender, 'ack 3 {0}'.format(data)))

                    # if get, try to get the value and return it in an ack to the sender
                    elif items[0] == 'get':
                        key = int(items[2])
                        if key in keyVal.keys():
                            q_toChannel.put((sender, 'ack 3 {0}| {1}| {2}'.format(data, keyVal[key][0], keyVal[key][1])))
                        else:
                            q_toChannel.put((sender, 'ack 3 {0}| {1}| {2}'.format(data, 0, -1.0)))
                            print("Can't get key {0}, doesn't exist".format(key))

                    #if repair send the information for the key-value store
                    elif items[0] == 'repair':
                        q_toChannel.put((sender, 'repairAck 3 |' + json.dumps(keyVal)))

                    # if an ack put into received queue to be handled by the command thread
                    elif items[0] == 'ack':
                        q_received.put(data)

                    elif items[0] == 'repairAck':
                        q_repair.put(data)

                    elif items[0] == 'repairWrite':
                        key = int(items[2])
                        value = int(items[3])
                        timestamp = float(items[4])
                        if (key in keyVal.keys() and keyVal[key][1] <= timestamp) or (key not in keyVal.keys()):
                            keyVal[key] = (value, timestamp)

                elif int(items[1]) <= 2:
                    # if model 1 or 2. Put delivered messages in q_delivered
                    # in keyValue functions, read q_delivered for linearizability
                    # and sequential consistency
                    # implement total ordering
                    if len(msg) == 3:
                        # sent from ABCD
                        dict_holdback[msg[2]] = msg[1]
                        # print 'put {0}:={1} in holdback dict'.format(msg[2], msg[1])
                    elif len(msg) == 4:
                        dict_order[msg[3]] = msg[2]
                        # print 'put {0}:={1} in order dict'.format(msg[3], msg[2])


                    # execute all msg exist
                    while True:
                        key = str(g_r_g)
                        # print 'to deliver :{0}:'.format(key)
                        # print dict_order.keys()


                        if key in dict_order.keys():
                            # put < msg_id, its data>
                            tmp_msg_id = dict_order[key]

                            # make sure the message is received
                            # otherwise wait
                            if tmp_msg_id in dict_holdback:
                                # print 'tmp_msg_id:={0}'.format(tmp_msg_id)
                                # print dict_holdback.keys()

                                # if not coming from self, process directly
                                # otherwise put in q_delivered, break, and let keyVal class handle
                                if list(tmp_msg_id)[0] == g_node_name:
                                    q_delivered.put((tmp_msg_id, dict_holdback[tmp_msg_id]))
                                    print 'put {0}:={1} in q_delivered'.format(tmp_msg_id, dict_holdback[tmp_msg_id])
                                    del dict_order[key]
                                    del dict_holdback[tmp_msg_id]
                                    g_r_g += 1

                                    break
                                else:
                                    items = dict_holdback[tmp_msg_id].split()
                                    # here process the message happened before my msg
                                    if items[0] == 'insert':
                                        keyVal[int(items[2])] = (int(items[3]), float(items[4]))
                                        # print 'processed sg_id insert:{0}:'.format(tmp_msg_id)
                                    elif items[0] == 'update':
                                        k = int(items[2])
                                        if k in keyVal.keys():
                                            keyVal[k] = (int(items[3]), float(items[4]))
                                            # print 'processed msg_id update:{0}:'.format(tmp_msg_id)
                                        else:
                                            print("Can't update key {0}, doesn't exist".format(key))
                                    elif items[0] == 'get':
                                        pass
                                        #print 'processed msg_id get:{0}:'.format(tmp_msg_id)
                                        # do nothing since will not change my values

                                    print 'processed {0}:={1}'.format(tmp_msg_id, dict_holdback[tmp_msg_id])
                                    del dict_order[key]
                                    del dict_holdback[tmp_msg_id]
                                    g_r_g += 1

                            else:
                                # if one of them is not true, break
                                break
                        else:
                            break

    def kill(self):
        self.is_alive = False


# delay function thread which generate random delays
class DelayThread(threading.Thread):
    def __init__(self, name, delay_dict, is_alive):
        threading.Thread.__init__(self)
        self.name = name
        self.delay_dict = delay_dict
        self.is_alive = is_alive

    def run(self):
	    # Simple channel list to ensure FIFO delivery of messages
    	delayQs = {}
    	delayQs['A'] = []
    	delayQs['B'] = []
    	delayQs['C'] = []
    	delayQs['D'] = []
        delayQs['S'] = []

        while self.is_alive:
            if not q_toChannel.empty():
                # Get delay and timestamp for new message and append to channel list
                item = q_toChannel.get()
                delay_max = int(self.delay_dict[self.name + item[0]])
                delay = random.randrange(0, delay_max + 1)
                delayQs[item[0]].append([item, delay, time.time()])

	        # send out messages from heads of each channel list that have been appropriately delayed
            for q_delay in delayQs.values():
                while len(q_delay) and time.time() >= q_delay[0][1] + q_delay[0][2]:
                    item = q_delay.pop(0)
                    q_toSend.put(item[0])

    def kill(self):
        self.is_alive = False


class keyValStore():
    def getModel1(self, key):
        # B-multicast <m, i>
        global g_msg_cnt
        global g_node_name
        g_msg_cnt += 1

        # generate message id
        msg_id = g_node_name + str(g_msg_cnt)

        #<sender; data; msg_id>
        sendString = "get 1 {0};{1}".format(key, msg_id)

        q_toChannel.put(('A', sendString))
        q_toChannel.put(('B', sendString))
        q_toChannel.put(('C', sendString))
        q_toChannel.put(('D', sendString))
        q_toChannel.put(('S', sendString))

        # wait until own get message is delivered
        while True:
            if not q_delivered.empty():
                tup = q_delivered.get()

                # print 'getModel1, deque :{0}:'.format(tup)

                items = tup[1].split()
                if tup[0] != msg_id:
                    # here process the message happened before my msg
                    if items[0] == 'insert':
                        keyVal[int(items[2])] = (int(items[3]), float(items[4]))
                    # elif items[0] == 'delete':
                    #     k = int(items[2])
                    #     if k in keyVal.keys():
                    #         del keyVal[k]
                    #     else:
                    #         print("Can't delete key {0}, doesn't exist".format(k))
                        print 'getModel1: added to keyValue :{0}:'.format(int(items[2]))
                    elif items[0] == 'update':
                        k = int(items[2])
                        if k in keyVal.keys():
                            keyVal[k] = (int(items[3]), float(items[4]))
                        else:
                            print("Can't update key {0}, doesn't exist".format(key))
                    elif items[0] == 'get':
                        pass
                        # do nothing since will not change my values
                elif tup[0] == msg_id:
                    k = int(items[2])

                    # print 'getkey :{0}:'.format(k)
                    # print keyVal.keys()

                    if k in keyVal.keys():
                        print keyVal[k]
                        return keyVal[k]
                    else:
                        print("Cannot get key {0}, doesn't exist".format(k))

    def getModel2(self, key):
        # immediately return the values
        print keyVal[key]
        return keyVal[key]

    def getModel3(self, key):
        sendString = "get 3 {0}".format(key)
        q_toChannel.put(('A', sendString))
        q_toChannel.put(('B', sendString))
        q_toChannel.put(('C', sendString))
        q_toChannel.put(('D', sendString))

        while(True):
            if not q_received.empty():
                items = q_received.get().split('|')
                if len(items) > 2 and items[0] == 'ack 3 ' + sendString:
                    print items[1]
                    return int(items[1])

    def getModel4(self, key):
        sendString = "get 4 {0}".format(key)
        q_toChannel.put(('A', sendString))
        q_toChannel.put(('B', sendString))
        q_toChannel.put(('C', sendString))
        q_toChannel.put(('D', sendString))
        r = 0
        x = []
        while(r != 2):
            if not q_received.empty():
                items = q_received.get().split('|')
                if len(items) > 2 and items[0] == 'ack 3 ' + sendString:
                    x.append((float(items[2]), int(items[1])))
                    r = r + 1
        print max(x)[1]
        return max(x)[1]


    def insertModel1(self, key, value):
        global g_msg_cnt
        global g_node_name
        g_msg_cnt += 1

        # generate message id
        msg_id = g_node_name + str(g_msg_cnt)
        # print msg_id

        #<sender; data; msg_id>
        sendString = "insert 1 {0} {1} {2};{3}".format(key, value, time.time(), msg_id)

        q_toChannel.put(('A', sendString))
        q_toChannel.put(('B', sendString))
        q_toChannel.put(('C', sendString))
        q_toChannel.put(('D', sendString))
        q_toChannel.put(('S', sendString))

        # wait until own insert msg comes
        while True:
            if not q_delivered.empty():
                tup = q_delivered.get()

                items = tup[1].split()
                if tup[0] != msg_id:
                    # here process the message happened before my msg
                    if items[0] == 'insert':
                        keyVal[int(items[2])] = (int(items[3]), float(items[4]))
                    # elif items[0] == 'delete':
                    #     k = int(items[2])
                    #     if k in keyVal.keys():
                    #         del keyVal[k]
                    #     else:
                    #         print("Can't delete key {0}, doesn't exist".format(k))
                        print 'getModel1: added to keyValue :{0}:'.format(int(items[2]))
                    elif items[0] == 'update':
                        k = int(items[2])
                        if k in keyVal.keys():
                            keyVal[k] = (int(items[3]), float(items[4]))
                        else:
                            print("Can't update key {0}, doesn't exist".format(k))
                    elif items[0] == 'get':
                        pass
                        # do nothing since will not change my values
                elif tup[0] == msg_id:
                    k = int(items[2])
                    keyVal[k] = (int(items[3]), float(items[4]))
                    print 'inserted {0}:={1}'.format(k, keyVal[k])
                    return keyVal[k]


    def insertModel2(self, key, value):
        global g_msg_cnt
        global g_node_name

        g_msg_cnt += 1

        # generate message id
        msg_id = g_node_name + str(g_msg_cnt)
        # print msg_id

        #<sender; data; msg_id>
        sendString = "insert 2 {0} {1} {2};{3}".format(key, value, time.time(), msg_id)

        q_toChannel.put(('A', sendString))
        q_toChannel.put(('B', sendString))
        q_toChannel.put(('C', sendString))
        q_toChannel.put(('D', sendString))
        q_toChannel.put(('S', sendString))

        # wait until own insert msg comes
        while True:
            if not q_delivered.empty():
                tup = q_delivered.get()

                items = tup[1].split()

                if tup[0] != msg_id:
                    # here process the message happened before my msg
                    if items[0] == 'insert':
                        keyVal[int(items[2])] = (int(items[3]), float(items[4]))
                    # elif items[0] == 'delete':
                    #     k = int(items[2])
                    #     if k in keyVal.keys():
                    #         del keyVal[k]
                    #     else:
                    #         print("Can't delete key {0}, doesn't exist".format(k))
                    elif items[0] == 'update':
                        k = int(items[2])
                        if k in keyVal.keys():
                            keyVal[k] = (int(items[3]), float(items[4]))
                        else:
                            print("Can't update key {0}, doesn't exist".format(k))
                    elif items[0] == 'get':
                        pass
                        # do nothing since will not change my values
                elif tup[0] == msg_id:
                    k = int(items[2])
                    keyVal[k] = (int(items[3]), float(items[4]))
                    print 'inserted {0}:={1}'.format(k, keyVal[k])
                    return keyVal[k]


    def insertModel3(self, key, value):
        sendString = "insert 3 {0} {1} {2}".format(key, value, time.time())
        q_toChannel.put(('A', sendString))
        q_toChannel.put(('B', sendString))
        q_toChannel.put(('C', sendString))
        q_toChannel.put(('D', sendString))
        w = 0
        while(w != 1):
            if not q_received.empty():
                item = q_received.get()
                if item == 'ack 3 ' + sendString:
                    w = w + 1

    def insertModel4(self, key, value):
        sendString = "insert 4 {0} {1} {2}".format(key, value, time.time())
        q_toChannel.put(('A', sendString))
        q_toChannel.put(('B', sendString))
        q_toChannel.put(('C', sendString))
        q_toChannel.put(('D', sendString))
        w = 0
        while(w != 2):
            if not q_received.empty():
                item = q_received.get()
                if item == 'ack 3 ' + sendString:
                    w = w + 1

    def updateModel1(self, key, value):
        global g_msg_cnt
        global g_node_name
        g_msg_cnt += 1

        # generate message id
        msg_id = g_node_name + str(g_msg_cnt)

        #<sender; data; msg_id>
        sendString = "update 1 {0} {1} {2};{3}".format(key, value, time.time(), msg_id)

        q_toChannel.put(('A', sendString))
        q_toChannel.put(('B', sendString))
        q_toChannel.put(('C', sendString))
        q_toChannel.put(('D', sendString))
        q_toChannel.put(('S', sendString))

        # wait until own insert msg comes
        while True:
            if not q_delivered.empty():
                tup = q_delivered.get()
                items = tup[1].split()
                if tup[0] != msg_id:
                    # here process the message happened before my msg
                    if items[0] == 'insert':
                        keyVal[int(items[2])] = (int(items[3]), float(items[4]))
                    # elif items[0] == 'delete':
                    #     k = int(items[2])
                    #     if k in keyVal.keys():
                    #         del keyVal[k]
                    #     else:
                    #         print("Can't delete key {0}, doesn't exist".format(k))
                    elif items[0] == 'update':
                        k = int(items[2])
                        if k in keyVal.keys():
                            keyVal[k] = (int(items[3]), float(items[4]))
                        else:
                            print("Can't update key {0}, doesn't exist".format(k))
                    elif items[0] == 'get':
                        pass
                        # do nothing since will not change my values
                elif tup[0] == msg_id:
                    k = int(items[2])
                    if k in keyVal.keys():
                            keyVal[k] = (int(items[3]), float(items[4]))
                    else:
                        print("Can't update key {0}, doesn't exist".format(k))

                    print 'updated {0}:={1}'.format(k, keyVal[k])
                    return keyVal[k]

    def updateModel2(self, key, value):
        global g_msg_cnt
        global g_node_name
        g_msg_cnt += 1

        # generate message id
        msg_id = g_node_name + str(g_msg_cnt)

        #<sender; data; msg_id>
        sendString = "update 2 {0} {1} {2};{3}".format(key, value, time.time(), msg_id)

        q_toChannel.put(('A', sendString))
        q_toChannel.put(('B', sendString))
        q_toChannel.put(('C', sendString))
        q_toChannel.put(('D', sendString))
        q_toChannel.put(('S', sendString))

        # wait until own insert msg comes
        while True:
            if not q_delivered.empty():
                tup = q_delivered.get()
                items = tup[1].split()
                if tup[0] != msg_id:
                    # here process the message happened before my msg
                    if items[0] == 'insert':
                        keyVal[int(items[2])] = (int(items[3]), float(items[4]))
                    # elif items[0] == 'delete':
                    #     k = int(items[2])
                    #     if k in keyVal.keys():
                    #         del keyVal[k]
                    #     else:
                    #         print("Can't delete key {0}, doesn't exist".format(k))
                    elif items[0] == 'update':
                        k = int(items[2])
                        if k in keyVal.keys():
                            keyVal[k] = (int(items[3]), float(items[4]))
                        else:
                            print("Can't update key {0}, doesn't exist".format(k))
                    elif items[0] == 'get':
                        pass
                        # do nothing since will not change my values
                elif tup[0] == msg_id:
                    k = int(items[2])
                    if k in keyVal.keys():
                            keyVal[k] = (int(items[3]), float(items[4]))
                    else:
                        print("Can't update key {0}, doesn't exist".format(k))

                    print 'updated {0}:={1}'.format(k, keyVal[k])
                    return keyVal[k]

    def updateModel3(self, key, value):
        sendString = "update 3 {0} {1} {2}".format(key, value, time.time())
        q_toChannel.put(('A', sendString))
        q_toChannel.put(('B', sendString))
        q_toChannel.put(('C', sendString))
        q_toChannel.put(('D', sendString))
        k = 0
        while(k != 1):
            if not q_received.empty():
                item = q_received.get()
                if item == 'ack 3 ' + sendString:
                    k = k + 1

    def updateModel4(self, key, value):
        sendString = "update 4 {0} {1} {2}".format(key, value, time.time())
        q_toChannel.put(('A', sendString))
        q_toChannel.put(('B', sendString))
        q_toChannel.put(('C', sendString))
        q_toChannel.put(('D', sendString))
        k = 0
        while (k != 2):
            if not q_received.empty():
                item = q_received.get()
                if item == 'ack 3 ' + sendString:
                    k = k + 1

    def delete(self, key):
        sendString = "delete {0}".format(key)
        q_toChannel.put(('A', sendString))
        q_toChannel.put(('B', sendString))
        q_toChannel.put(('C', sendString))
        q_toChannel.put(('D', sendString))
        k = 0
        while (k != 4):
            if not q_received.empty():
                item = q_received.get()
                if item == 'deleteAck {0}'.format(key):
                    k = k + 1

    def delay(self, delayTime):
        start = time.time()
        while(time.time() - start < delayTime):
            pass

    def showall(self):
        print keyVal

    def search(self, key):
        sendString = "search {0}".format(key)
        q_toChannel.put(('A', sendString))
        q_toChannel.put(('B', sendString))
        q_toChannel.put(('C', sendString))
        q_toChannel.put(('D', sendString))

        servers = []
        for i in range(0, 4):
            items = q_search.get(block=True).split()
            if items[1] != 'F':
                servers.append(items[1])
        print servers


# command line interface thread
class CmdThread(threading.Thread):
    def __init__(self, name):
        threading.Thread.__init__(self)
        # self.threadID = thread_id
        self.name = name

    def run(self):
        print "Starting command line interface..."

        # create command line interface object
        cli = MP1Shell()
        print 'created cmd line interface object'
        cli.cmdloop()
        # print 'started command line interface'
        return True


# command line class
class MP1Shell(cmd.Cmd, keyValStore):
    intro = 'Welcome to the MP1 shell,' \
            ' type help or ? to list commands'
    prompt = '(MP1) '
    file = None

    # ------- basic commands -------------
    def do_do(self, arg):
        doFile = open(arg, "r")
        for line in doFile:
            item = line.split()
            if item[0] == 'insert':
                self.do_insert(line[7:])
            if item[0] == 'update':
                self.do_update(line[7:])
            if item[0] == 'get':
                self.do_get(line[4:])
            if item[0] == 'delete':
                self.do_delete(line[7:])
            if item[0] == 'delay':
                self.do_delay(line[6:])
            if item[0] == 'search':
                self.do_search(line[7:])
            if item[0] == 'show-all':
                self.do_showall(line[9:])

    def do_delete(self, arg):
        tp = arg.split()
        if len(tp) < 1:
            print("not enough parameters")
            return
        try:
            key = int(tp[0])
        except ValueError:
            print("key must be an integer")
            return
        self.delete(key)

    def do_delay(self, arg):
        tp = arg.split()
        if len(tp) < 1:
            print("not enough parameters")
            return
        try:
            wait = float(tp[0])
        except ValueError:
            print("time must be a number")
            return
        self.delay(wait)

    def do_search(self, arg):
        tp = arg.split()
        if len(tp) < 1:
            print("not enough parameters")
            return
        try:
            key = int(tp[0])
        except ValueError:
            print("key must be an integer")
            return
        self.search(key)

    def do_showall(self, arg):
        self.showall()

    def do_get(self, arg):
        tp = arg.split()
        if len(tp) < 2:
            print("not enough parameters")
            return
        try:
            key = int(tp[0])
        except ValueError:
            print("key must be an integer")
            return

        if tp[1] == '1':
            self.getModel1(key)
        elif tp[1] == '2':
            self.getModel2(key)
        elif tp[1] == '3':
            self.getModel3(key)
        elif tp[1] == '4':
            self.getModel4(key)
        else:
            print("invalid model")

    def do_insert(self, arg):
        tp = arg.split()
        if len(tp) < 3:
            print("not enough parameters")
            return
        try:
            key = int(tp[0])
            value = int(tp[1])
        except ValueError:
            print("key/value must be an integer")
            return
        if tp[2] == '1':
            self.insertModel1(key, value)
        elif tp[2] == '2':
            self.insertModel2(key, value)
        elif tp[2] == '3':
            self.insertModel3(key, value)
        elif tp[2] == '4':
            self.insertModel4(key, value)
        else:
            print("invalid model")

    def do_update(self, arg):
        tp = arg.split()
        if len(tp) < 3:
            print("not enough parameters")
            return
        try:
            key = int(tp[0])
            value = int(tp[1])
        except ValueError:
            print("key/value must be an integer")
            return

        if tp[2] == '1':
            self.updateModel1(key, value)
        elif tp[2] == '2':
            self.updateModel2(key, value)
        elif tp[2] == '3':
            self.updateModel3(key, value)
        elif tp[2] == '4':
            self.updateModel4(key, value)
        else:
            print("invalid model")

    def do_Send(self, arg):
        """
        :param arg: Send Message Destination
        :return: Sent "Hello" to B, system time is ...
        """
        tp = arg.split()
        q_toChannel.put((tp[1], tp[0]))
        print 'Send "' + tp[0] + '" to ' + tp[1] + ', system time is ' + \
            time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

    def do_show(self, arg):
        """
        show the q_toSend and q_toChannel for debugging
        :param arg: toSend, toChannel
        :return: print q_toSend, q_toChannel
        """
        if arg == 'toSend':
            q_toSend_copy = q_toSend
            if not q_toSend_copy.empty():
                while not q_toSend_copy.empty():
                    print q_toSend_copy.get()
            else:
                print 'q_toSend is empty'

        if arg == 'toChannel':
            q_toChannel_copy = q_toChannel
            if not q_toChannel_copy.empty():
                while not q_toChannel_copy.empty():
                    print q_toChannel_copy.get()
            else:
                print 'q_toChannel is empty'

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
    global g_node_name
    if len(argv) != 2:
        print 'Specify config file and node name'
        sys.exit(0)

    print str(argv)

    config_file = str(argv[0])
    node_name = argv[1]

    # set as global node name for use in other thread
    g_node_name = node_name

    # print config_file

    # first read configuration file to set it up
    f = open(config_file, 'r')
    config = csv.reader(f)
    # print 'read done'
    header_config = config.next()

    # save name, IP, and ports in dic
    node_dict = {}
    delay_dict = {}
    dict_ip_done = False
    for row in config:
        # print row

        if row[0] == 'Channel':
            dict_ip_done = True
            continue

        if not dict_ip_done:
            name = row[0]
            ip = row[1]
            port = row[2]
            node_dict[name] = [ip, port]

        else:
            chn = row[0]
            max_delay = row[1]
            delay_dict[chn] = max_delay

    f.close()

    # start a thread for listening and receiving packages
    tu = node_dict[node_name]
    HOST = tu[0]
    PORT = int(tu[1])
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((HOST, PORT))

    print node_name
    print HOST
    print PORT

    recv_thread = ReceiveThread(node_name, sock, node_dict, delay_dict, True)
    print 'created receive thread'
    recv_thread.start()

    # Here start the thread for checking if should send out messages
    send_thread = SendThread(node_dict, node_name, True)
    print 'created send thread'
    send_thread.start()

    # Here start the delay thread
    delay_thread = DelayThread(node_name, delay_dict, True)
    print 'created delay thread'
    delay_thread.start()

    # Here start the thread for command line interface
    shell_thread = CmdThread('MP1Shell')
    print 'created shell thread'
    shell_thread.start()

    shell_thread.join()
    print 'after join'

    recv_thread.kill()
    send_thread.kill()
    delay_thread.kill()
    print 'three kills'


    recv_thread.join()
    print 'killed recv'
    send_thread.join()
    print 'killed send'
    delay_thread.join()
    print 'killed delay'



if __name__ == '__main__':
    main(sys.argv[1:])