##########################################################################################
# 
# Name: Hongbo Lin
# Class: ICS 421 Database System II
# Professor: Lipyeow Lim
# Date: Feburary 15, 2018
# Assignment: #2 (for detail, please see 
#                https://lipyeow.github.io/ics421s18/morea/queryproc/experience-hw2.html)
# File: runSQL.py
# Desciption: runSQL program that executes a given DDL statement on a cluster of computer
#             each running an instance of a DBMS
#
##########################################################################################

####################################################################
# NOTES: The program runs on Python 3 and uses sqlite3 as database #
####################################################################

# Import all necessary library
import threading
import pickle
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
            Define the run function for the ConnectThread
            Create a socket and connect to server which host and port are given by
            self.node. Send string "runSQL.py for server to distinguish the type of
            program connection, send self.node for server to distinguish it is node
            or catalog. Then send the sqlcmd for server to execute and waiting for 
            the data, next output success or fail
        '''

        # create socket and connect
        mySocket = socket.socket()
        mySocket.connect((self.node['host'], int(self.node['port'])))

        # send "runSQL.py", node, and sqlcmd        
        mySocket.send(pickle.dumps("runSQL.py"))
        time.sleep(0.1)
        mySocket.send(pickle.dumps(self.node))
        time.sleep(0.1)
        mySocket.send(pickle.dumps(self.sqlcmd))
     
        # receive the message or data
        message = pickle.loads(mySocket.recv(1024))      

        # print result
        if message[0] is not None:
            for i in range(len(message[0])):
                m = str(message[0][i])
                print(m.replace("(", "").replace(")", "").replace("'", "").replace(",", ""))

        # set self._return to corresponding fail or succes message
        if message[1] == "Failed":
            self._return = '[{0}:{1}/{2}]: ./{3} failed'.format(self.node['host'], self.node['port'], self.node['database'], self.node['tname'])
        else:
            self._return = '[{0}:{1}/{2}]: ./{3} success'.format(self.node['host'], self.node['port'], self.node['database'], self.node['tname'])

    
    def join(self):
        '''
            Define the join function
            return:
                either success or failed message
        ''' 
        threading.Thread.join(self)
        return self._return

def readCFG(clustercfg):
    '''
        Open the clustercfg file and parse info of catalog DB
        
        Parameter:
            clustercfg: The clustercfg file to be parsed

        Return:
            catalog: a dict that contains information of catalog DB
    '''
    # try open clustercfg file and parse infomation of catalog DB
    try:
        with open(clustercfg) as cluster:
            for line in cluster:
                if line != '\n':
                    temp = line.split("=")
                    if "driver" in temp[0]:
                        driver = temp[1].replace("\n", "")
                    elif "username" in temp[0]:
                        username = temp[1].replace("\n", "")
                    elif "passwd" in temp[0]:
                        passwd = temp[1].replace("\n", "")
                    elif "hostname" in temp[0]:
                        tmp = re.split(':|/', temp[1].replace("\n", ""))
                        host = tmp[0]
                        port = int(tmp[1])
                        database = tmp[2]

            # catalog dict contain all information about catalog DB
            catalog = {
                'driver': driver,
                'host': host,
                'port': port,
                'database': database,
                'username': username,
                'passwd': passwd,
            }
          
    # handle exception         
    except IOError:
        print("Unable to open file: " + clustercfg)
        sys.exit()

    # clean up
    finally:
        # print(catalog)
        cluster.close()

    # return catalog dict
    return catalog

def readSQL(sqlfile):
    '''
       Parse the sqlfile and get the sql command and table name

       Parameter:
           sqlfile: The sqlfile to be parse

       Returns: 
           sqlcontent: The SQL command
           tname: The name of table SQL on
    '''
    # open sqlfile, read the content and get table name
    try:
        sql = open(sqlfile, 'r')
        sqlcontent = sql.read()
        tname = sqlcontent.split("FROM ")[1].replace(";", "").replace("\n", "")

    # handle exception
    except IOError:
        print("Unable to open file: " + sqlfile)
        sys.exit()

    # clean up
    finally:
        # print(sqlcontent)
        # print(tname)
        sql.close()

    # return sql command and table name
    return sqlcontent, tname

# The main program
if __name__ == '__main__':

    # check if correct number argument are passed in
    if len(sys.argv) != 3:
        print("Invalid command line arguments")
        print("Usage: python3 runSQL.py <clustercfg> <sqlfile>")
        print("       <clustercfg>: contains access information for the catalog DB")
        print("       <sqlfile>: contain the SQL terminated by a semi-colon to be execuated")
        sys.exit()

    # read in the arguments
    clustercfg = sys.argv[1]
    sqlfile = sys.argv[2]

    # read both file and obtain info
    catalog = readCFG(clustercfg)
    sqlcontent, tname = readSQL(sqlfile)

    # try connect to catalog database to get nodes info
    try:
        # connect to catalog
        catalogSocket = socket.socket()
        catalogSocket.connect((catalog['host'], catalog['port']))

        # send necessary stuffs to catalog server
        catalogSocket.send(pickle.dumps("runSQL.py"))
        time.sleep(0.1)
        catalogSocket.send(pickle.dumps(catalog))
        time.sleep(1)
        catalogSocket.send(pickle.dumps(tname))

        # receive nodes info
        nodelist = pickle.loads(catalogSocket.recv(1024))

        # if something went wrong when accessing catalog database on server
        if nodelist == "Fail":
            print("Something went wrong when accessing catalog database")
            sys.exit()

        # else:
        #    print(nodelist)

    # handle exception
    except socket.error as e:
        print("Something went wrong during connection to catalog database.")
        print(e)
        sys.exit()

    # clean up
    finally:
        catalogSocket.close()

    # number of nodes obtained
    num = len(nodelist)

    # nodes list for store node dict later
    nodes = []

    # looping to parse nodelist and create dict for each node
    for i in range(num):
        tmp = re.split(':|/', nodelist[i][2].replace("\n", ""))
        node = {
            'tname': nodelist[i][0],
            'driver': nodelist[i][1],
            'host': tmp[0],
            'port': tmp[1],
            'database': tmp[2],
            'nodeid': nodelist[i][3]
        }
        nodes.append(node)


    # for i in range(num):
    #    print(nodes[i])

    # threads list    
    threads = []

    # infolist for thread return
    infolist = []

    # start a new thread for each node
    for i in range(len(nodelist)):
        newThread = ConnectThread(nodes[i], sqlcontent)
        newThread.start() 
        threads.append(newThread)

    # wait for threads to finish and append to infolist
    for t in threads:
        info = t.join()
        infolist.append(info)

    # print return message from threads
    for i in range(len(infolist)):
        print(infolist[i])


