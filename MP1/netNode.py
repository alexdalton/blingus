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

# define global queues
# to be pushed to channel (delay happens in channel)
# a tuple, destination(ABCD), and message
q_toChannel = Queue.Queue()

# to be send out (after delay)
# a tuple: destination (ABCD), message
q_toSend = Queue.Queue()


# the client thread which constantly read q_toSend and send out messages
class SendThread(threading.Thread):
    def __init__(self, node_dict, name):
        threading.Thread.__init__(self)
        self.node_dict = node_dict
        self.node_name = name

    def run(self):
        # print "Starting sending thread"

        while True:
            if not q_toSend.empty():
                item = q_toSend.get()
                dest = self.node_dict[item[0]]

                UDP_IP = dest[0]
                UDP_PORT = int(dest[1])
                MESSAGE = self.node_name + ',' + item[1]

                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.sendto(MESSAGE, (UDP_IP, UDP_PORT))

            else:
                time.sleep(0.1)


# the server thread class which listens incoming messages
# create a thread to handle incoming messages once sock bind
class ReceiveThread(threading.Thread):
    def __init__(self, name, sock, node_dict, delay_dict):
        threading.Thread.__init__(self)
        self.name = name
        self.sock = sock
        self.node_dict = node_dict
        self.delay_dict = delay_dict

    def run(self):
        # print 'node ' + self.name + ' starts listening:'

        while True:
            msg_str, addr = self.sock.recvfrom(1024)

            msg = msg_str.split(',')

            sender = msg[0]
            data = msg[1]

            # find out the maximal delay
            chn = sender + self.name
            if self.delay_dict[chn] is None:
                print 'invalid channel' + chn
            else:
                delay_max = int(self.delay_dict[chn])

            # print message
            print 'Received "' + data + '" from ' + sender + ', Max delay is ' \
                + str(delay_max) + 's, system time is ' + \
                time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())


# delay function thread which generate random delays
class DelayThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        while True:
            if not q_toChannel.empty():
                item = q_toChannel.get()
                time.sleep(1)
                q_toSend.put(item)


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
        print 'started command line interface'


# command line class
class MP1Shell(cmd.Cmd):
    intro = 'Welcome to the MP1 shell,' \
            ' type help or ? to list commands'
    prompt = '(MP1) '
    file = None

    # ------- basic commands -------------
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

    def do_byebye(self, arg):
        """
        :param arg: bye (stop code)
        :return: true
        """
        print 'stop demo'
        # sys.exit(0)


def main(argv):
    if len(argv) != 2:
        print 'Specify config file and node name'
        sys.exit(0)

    print str(argv)

    config_file = str(argv[0])
    node_name = argv[1]

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

    recv_thread = ReceiveThread(node_name, sock, node_dict, delay_dict)
    print 'created receive thread'
    recv_thread.start()

    # Here start the thread for command line interface
    shell_thread = CmdThread('MP1Shell')
    print 'created shell thread'
    shell_thread.start()

    # Here start the thread for checking if should send out messages
    send_thread = SendThread(node_dict, node_name)
    print 'created send thread'
    send_thread.start()

    # Here start the delay thread
    delay_thread = DelayThread()
    print 'created delay thread'
    delay_thread.start()


if __name__ == '__main__':
    main(sys.argv[1:])



