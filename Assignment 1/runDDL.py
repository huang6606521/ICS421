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
import sys
import re

# Thread class for a new connection
class ConnectThread(threading.Thread):
    
    def __init__(self, node, sqlcmd):
        '''
            Define the __init__ function for the thread
            node: contains the information of node such as host, port, database...
            sqlcmd: the sql command the server need to execute
        '''
        threading.Thread.__init__(self)
        self.node = node
        self.sqlcmd = sqlcmd

    def run(self):
        '''
            Define the run function for the thread.
            Create a socket and connect to the server which host and port are given by
            self.node. Send self.node over for server to distinguish if it is node or
            catalog. Send the database name and the sql command for server to executed.
            Then waiting for servers to finish execution and recv a message whether is
            Success or Fail. Print the output to console according to the message.
        '''
        mySocket = socket.socket()
        mySocket.connect((self.node['host'], self.node['port']))
        
        mySocket.send(self.node['database'].encode())
        mySocket.recv(1024).decode()
        mySocket.send(self.sqlcmd.encode())
        message = mySocket.recv(1024).decode()
        if message.find("Success") != -1:
            print('[{0}:{1}/{2}]: ./{3} success.'.format(self.node['host'], self.node['port'], self.node['database'], self.node['sqlfile']))
        elif message.find("Fail") != -1:
            print('[{0}:{1}/{2}]: ./{3} failed.'.format(self.node['host'], self.node['port'], self.node['database'], self.node['sqlfile']))
        mySocket.close()

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
                # if line contains catalog
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
                # if line contains node
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

# Create a connectThread for each node and run
for i in range(num):
    newThread = ConnectThread(nodes[i], ddlcontent)
    newThread.start()
    threads.append(newThread)

# Wait for all threads to finish tasks
for t in threads:
    t.join()



