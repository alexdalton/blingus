__author__ = 'Yanning Li'
# this code implements a network node, which listens
# (as a server), sends messages (as a client),
# with delay function, and
# command line interface

import socket
import sys
import threading
import time
import Queue
import csv


# messages
# sender; message; message_id


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
                # print 'length of item is {0}'.format(len(item))
                MESSAGE = self.node_name + ';' + item[1]

                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.sendto(MESSAGE, (UDP_IP, UDP_PORT))

            else:
                # print 'wait to send'
                time.sleep(0.1)


# the server thread class which listens incoming messages
# create a thread to handle incoming messages once sock bind
class ReceiveThread(threading.Thread):
    def __init__(self, name, sock, node_dict, s_g):
        threading.Thread.__init__(self)
        self.name = name
        self.sock = sock
        self.node_dict = node_dict
        self.s_g = s_g

    def run(self):
        # print 'node ' + self.name + ' starts listening:'

        while True:
            msg_str, addr = self.sock.recvfrom(1024)
            msg = msg_str.split(';')

            sender = msg[0]
            data = msg[1]
            msg_id = msg[2]

            # send out string to node
            # <S; data; msg_id; s_g>
            sendString = "{0};{1};{2}".format( data, msg_id, self.s_g)

            # to be consistent with the ABCD side
            self.s_g += 1

            # send out the message to all four nodes as a tuple
            # no delay
            q_toSend.put(('A', sendString))
            q_toSend.put(('B', sendString))
            q_toSend.put(('C', sendString))
            q_toSend.put(('D', sendString))

            print 'broadcasted msg {0} with sequencer {1}'.format(msg_id, self.s_g)


def main(argv):
    if len(argv) != 2:
        print 'Specify config file and node name'
        sys.exit(0)

    print str(argv)

    config_file = str(argv[0])
    node_name = argv[1] # node name is S

    s_g = 0

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

    recv_thread = ReceiveThread(node_name, sock, node_dict, s_g)
    print 'created receive thread'
    recv_thread.start()


    # Here start the thread for checking if should send out messages
    send_thread = SendThread(node_dict, node_name)
    print 'created send thread'
    send_thread.start()



if __name__ == '__main__':
    main(sys.argv[1:])



