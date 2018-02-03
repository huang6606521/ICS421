##########################################################################################
# 
# Name: Hongbo Lin
# Class: ICS 421 Database System II
# Professor: Lipyeow Lim
# Date: Janauary 25, 2018
# Assignment: #1 (for detail, please see 
#                 https://lipyeow.github.io/ics421s18/morea/pardb/experience-hw1.html)
# File: runDDL.py
# Desciption: runDDL program that executes a given DDL statement on a cluster of computer
#             each running an instance of a DBMS
#
##########################################################################################

####################################################################
# NOTES: The program runs on Python 3 and uses sqlite3 as database #
####################################################################

# Import all necessary library
import threading
import socket
import time
import sys
import re

# Thread class for a new connection
class ConnectThread(threading.Thread):
    
    def __init__(self, node, sqlcmd):
        '''
            Define the __init__ function for the ConnectThread
            node: contains the information of node such as host, port, database...
            sqlcmd: the sql command the server need to execute
            _return: some info to return for the thread
        '''
        threading.Thread.__init__(self)
        self.node = node
        self.sqlcmd = sqlcmd
        self._return = None

    def run(self):
        '''
            Define the run function for the ConnectThread.
            Create a socket and connect to the server which host and port are given by
            self.node. Send self.node over for server to distinguish if it is node or
            catalog. Send the database name and the sql command for server to executed.
            Then waiting for servers to finish execution and recv a message whether is
            Success or Fail. Print the output to console according to the message.
        '''
        mySocket = socket.socket()
        mySocket.connect((self.node['host'], self.node['port']))
        
        mySocket.send(str(node).encode())
        time.sleep(0.1)
        mySocket.send(self.node['database'].encode())
        time.sleep(0.1)
        mySocket.send(self.sqlcmd.encode())
        message = mySocket.recv(1024).decode()
        
        # print and update self._return base on success or fail
        if message.find("Success") != -1:
            print('[{0}:{1}/{2}]: ./{3} success.'.format(self.node['host'], self.node['port'], self.node['database'], self.node['sqlfile']))
            self._return = self.node['driver'] + " " + str(self.node['nodeid'])
        elif message.find("Fail") != -1:
            print('[{0}:{1}/{2}]: ./{3} failed.'.format(self.node['host'], self.node['port'], self.node['database'], self.node['sqlfile']))
    
    
        mySocket.close()

    # The join idea of return something is from
    # https://stackoverflow.com/questions/6893968/how-to-get-the-return-value-from-a-thread-in-python#answer-6894023
    def join(self):
        '''
            Define the join function for the ConnectThread
            call the join function from Thread library and return self._return which is
            a string of driver and nodeid when success or None when fail
        '''
        threading.Thread.join(self)
        return self._return

# Check if correct number argument are passed in
if len(sys.argv) != 3:
    print("Invalid command line argument")
    print("Usage: python3 runDDL.py <clustercfg> <ddlfile>")
    print("       <clustercfg>: contain access information for each computer on the cluster")
    print("       <ddlfile>: contain DDL terminated by a semi-colon to be executed")
    sys.exit()

clustercfg = sys.argv[1]
ddlfile = sys.argv[2]

# Open ddl file or file cannot open
try:
    ddl = open(ddlfile, 'r')
except IOError:
    print("Unable to open file: " + ddlfile)
    sys.exit()

# read the content of ddlfile
ddlcontent = ddl.read()

# nodes list, contains information of nodes after parsing
nodes = []

# Open the clustercfg file and parse all the information of catalog, nodes
# or file cannot open
try:
    with open(clustercfg) as cluster:
        for line in cluster:
            if line != '\n':
                temp = line.split("=")
                # if line contains numnodes
                if temp[0].find("numnodes") != -1:
                    num = int(temp[1])
                # if line contains catalog, get info and create catalog dict
                elif temp[0].find("catalog") != -1:
                    if temp[0].find("driver") != -1:
                        driver = temp[1].replace("\n", "")
                    elif temp[0].find("hostname") != -1:
                        tmp = re.split(':|/', temp[1].replace("\n", ""))
                        host = tmp[0]
                        port = int(tmp[1])
                        db = tmp[2]
                        catalog = {
                            'driver': driver,
                            'host': host,
                            'port': port,
                            'database': db
                        }
                # if line contains node, get each info and create node dict
                elif temp[0].find("node") != -1:
                    if temp[0].find("driver") != -1:
                        driver = temp[1].replace("\n", "")
                        nodeid = int(re.search(r'\d+', temp[0]).group())
                    elif temp[0].find("hostname") != -1:
                        tmp = re.split(':|/', temp[1].replace("\n", ""))
                        host = tmp[0]
                        port = int(tmp[1])
                        db = tmp[2]
                        node = {
                            'driver': driver,
                            'nodeid': nodeid,
                            'host': host,
                            'port': port,
                            'database': db,
                            'sqlfile': ddlfile
                        }
                        nodes.append(node)
except IOError:
    print("Unable to open file: " + clustercfg)
    sys.exit()

# print(catalog)
# print(nodes)

# Check if having correct number of nodes
if num != len(nodes):
    print("Node number does not match")
    sys.exit()

# close files
cluster.close()
ddl.close()

# threads list holding all threads created
threads = []

# info list contains return value from thread
infolist = []

# Create a connectThread for each node and run
for i in range(num):
    newThread = ConnectThread(nodes[i], ddlcontent)
    newThread.start()
    threads.append(newThread)

# Wait for all threads to finish tasks, append the return value to infolist
# only if it is not None
for t in threads:
    info = t.join()
    if info is not None:
        infolist.append(info)

# get sql cmd is create or drop
c_or_d = ddlcontent.split(" TABLE ")[0]

# get the table name
tname = re.split('\(|;', ddlcontent.split(" TABLE ")[1])[0]

length = len(infolist)

# add c_or_d and tname to each string in infolist
for i in range(length):
    infolist[i] = c_or_d + " " + tname + " " + infolist[i]

try:
    catalogSocket = socket.socket()
    catalogSocket.connect((catalog['host'], catalog['port']))

    # time.sleep() is a stupid way to send multiple message to server to recv
    # and wait
    catalogSocket.send(str(catalog).encode())
    time.sleep(0.1)
    catalogSocket.send(catalog['database'].encode())
    time.sleep(0.1)
    catalogSocket.send(str(length).encode())
    time.sleep(0.1)

    # loop to send info of each update
    for i in range(length):
        catalogSocket.send(infolist[i].encode())
        time.sleep(0.1)

    # receive message from catalog if updated
    msg = catalogSocket.recv(1024).decode()

    # print message on success or fail
    if msg.find("Updated") >= 0:
        print('[{0}:{1}/{2}]: catalog updated.'.format(catalog['host'], catalog['port'], catalog['database']))
    elif msg.find("Fail" >= 0):
        print('[{0}:{1}/{2}]: catalog error on update.'.format(catalog['host'], catalog['port'], catalog['database']))
except socket.Error:
    print("Something wrong during connect to catalog database")
finally:
    catalogSocket.close()



