__author__ = 'emlynlyn'
# this code implement a network node, which listens (as a server), sends messages (as a client), with delay function, and
# command line interface

import socket
import sys
import threading
import time
import cmd


# Three classes to be written
# Channel Class which handles the delay and communication
# Interface Class which handles the cmd line interface
# Node Class, which creates nodes in the network

# Those classes can use the following thread classes to
# assign tasks to each thread



# the client thread class which sends message
class ServerComThread(threading.Thread):
    def __init__(self, thread_id, name, sock):
        threading.Thread.__init__(self)
        self.threadID = thread_id
        self.name = name
        self.sock = sock
        self.counter = 5

    def run(self):
        print "Starting " + self.name
        # threadLock.acquire()
        # send packages for 5 rounds
        server_com(self.name, self.sock, 5)
        # threadLock.release()


def server_com(thread_name, sock, counter):
    # never close the socket
    # assume data can be transmitted in 1024
    client_data = sock.recv(1024)
    # print the length of the received data
    # print len(client_data)
    if not client_data:
        print 'server: no data received'
    # time.sleep(1)
    print 'server: talking with', thread_name
    # sock.send(client_data + 'received by' + thread_name)
    sock.close()


# the server thread class which listens incoming connections
class ServerThread(threading.Thread):
    def __init__(self, name, sock):
        threading.Thread.__init__(self)
        self.name = name
        self.sock = sock

    def run(self):
        print 'server starts listening' + self.name
        # threadLock.acquire()
        # send packages for 5 rounds
        server_listen(self.name, self.sock)
        # threadLock.release()


def server_listen(thread_name, sock, counter):
    # accept connections
    num_connection_server = 0
    while 1:
        conn_s, addr_s = sock.accept()
        print 'Connected by ', addr_s
        num_connection_server += 1

        # assign a new thread to handle the communication
        # read in the data
        # send back ack
        tmp_thread = ServerComThread('Thread-'+str(num_connection_server), conn_s)
        tmp_thread.start()
        print 'created thread to hand incoming msg'


# delay function thread which generate random delays
class DelayThread(threading.Thread):
    def __init__(self, threadID, name, sock):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.sock = sock
        self.counter = 5

    def run(self):
        print "Starting " + self.name
        # threadLock.acquire()
        # send packages for 5 rounds
        # sockCom(self.name, self.sock, 5)
        # threadLock.release()


# command line interface thread
class CmdThread(threading.Thread):
    def __init__(self, name):
        threading.Thread.__init__(self)
        # self.threadID = thread_id
        self.name = name

    def run(self):
        print "Starting command line interface..."

        # create command line interface object
        cli = NetCmdShell()
        print 'created cmd line interface object'
        cli.cmdloop()
        print 'started command line interface'


# command line class
class NetCmdShell(cmd.Cmd):
    intro = 'Welcome to the net command line shell,' \
            ' type help or ? to list commands'
    prompt = '(NetCmd)'
    file = None

    # ------- basic commands -------------
    def do_send(self, arg):
        print 'send message'

    def do_recv(self, arg):
        print 'receive message'


def parse(arg):
    print 'parse the argument'

# to figure out what the following means
if __name__ == '__main__':
    NetCmdShell().cmdloop()



HOST = None
PORT = 50007
s = None

threadLock = threading.Lock()
threads = []

for res in socket.getaddrinfo(HOST, PORT, socket.AF_UNSPEC,
                              socket.SOCK_STREAM, 0, socket.AI_PASSIVE):

    af, socktype, proto, canonname, sa = res
    try:
        print 'server: try to create new socket'
        s = socket.socket(af, socktype, proto)
    except socket.error as msg:
        print 'server: could not create new socket'
        print msg
        s = None
        continue
    try:
        print 'server: try to bind s to sa'
        s.bind(sa)
        print 'server: listening port 50007'
        s.listen(5)
    except socket.error as msg:
        print 'server: could not bind or no connection'
        print msg
        s.close()
        s = None
        print 'server: s closed'
        continue
    break

if s is None:
    print 'server: could not open socket'
    sys.exit(1)

# Now assign the bind socked s to a thread with the following server function
server_thread = ServerThread('main-server-thread', s)
print 'created main server thread'
server_thread.start()
print 'started main server thread'

# create a server thread
server_thread = ServerThread('server-thread', s)

# start command line interface
# now we should send messages over nodes
cmd_thread = CmdThread('command-line-interface')
print 'created command line interface thread'

