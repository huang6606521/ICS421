##########################################################################################
# 
# Name: Hongbo Lin
# Class: ICS 421 Database System II
# Professor: Lipyeow Lim
# Date: Feburary 15, 2018
# Assignment: #2 (for detail, please see 
#                https://lipyeow.github.io/ics421s18/morea/queryproc/experience-hw2.html)
# File: parDBd.py
# Desciption: parDBd.py which is a server program react depends on the type of connection,
#             whether a runSQL or loadCSV to a node or catalog
# 
##########################################################################################

####################################################################
# NOTES: The program runs on Python 3 and uses sqlite3 as database #
####################################################################

# Import all necessary library
import threading
import sqlite3
import pickle
import socket
import time
import sys

# ListenThread class for new incoming connection for client
class ListenThread(threading.Thread):

    def __init__(self, ip, port, clientSocket):
        '''
            Define the __init__ function for the ListenThread
            ip: the ip address(hostname) of client
            port: the port number of client
            csocket: the client socket
        '''
        threading.Thread.__init__(self)
        self.ip = ip
        self.port = port
        self.csocket = clientSocket
        print("New thread started for " + ip + " " + str(port))

    def run(self):
        '''
            Define the run function for the ListenThread
            if runSQL
                if catalog server
                    Receive the tname and connect to mycatdb. Obtain the all information
                    of nodes match the tname such as nodeurl, nodeid etc. and sent back 
                    to client
                if node server
                    Receive the sql command and connect to database. Obtain the table row
                    and sent back to client, also report success or fail
            if loadCSV
                if catalog server

                if node server

        '''
        # runSQL or loadCSV
        r_or_l = pickle.loads(self.csocket.recv(1024))

        if r_or_l == 'runSQL.py':
            # receive from client the dict whether a node or dict
            c_or_n = pickle.loads(self.csocket.recv(1024))
            print(c_or_n)
   
            # catalog server 
            if c_or_n.get('nodeid') is None:
                # get the database name
                dbfile = c_or_n['database']

                # receive table name
                tablename = pickle.loads(self.csocket.recv(1024))
                print(tablename)

                # try to connect to database and execute sql command to obtain information
                # of nodes match table name
                try:
                    dbconn = sqlite3.connect(dbfile)
                    cur = dbconn.cursor()
                    cur.execute("SELECT tname, nodedriver, nodeurl, nodeid FROM DTABLES WHERE tname = '{0}';".format(tablename)) 
                    nodelist = cur.fetchall()
    
                    self.csocket.send(pickle.dumps(nodelist))
                    print(nodelist)

                # send fail message if error
                except sqlite3.Error:
                    self.csocket.send(pickle.dumps("Fail"))
    
                # clean up
                finally:
                    cur.close()
                    dbconn.commit()
                    dbconn.close()

            # else node server
            else:
                # get database name
                db = c_or_n['database']
    
                # receive sql command
                sqlcmd = pickle.loads(self.csocket.recv(1024))
    
                # connect to databse and execute sql command to get data stored
                try:
                    dbconn = sqlite3.connect(db)
                    cur = dbconn.cursor()
                    cur.execute(sqlcmd)
                    result = cur.fetchall()
    
                    # send result(the data) and success message
                    self.csocket.send(pickle.dumps([result, "Success"]))
    
                # send fail message if error
                except sqlite3.Error:
                    self.csocket.send(pickle.dumps([None, "Failed"]))
    
                # clean up
                finally:
                    cur.close()
                    dbconn.commit()
                    dbconn.close()

        # loadCSV
        if r_or_l == 'loadCSV.py':
            # receive from client the dict whether a node or dict
            c_or_n = pickle.loads(self.csocket.recv(1024))
            print(c_or_n)
            
            # catalog server
            if c_or_n.get('nodeid') is None:
                # get the database name
                dbfile = c_or_n['database']
                
                # receive table name
                tablename = c_or_n['tablename']
                print(tablename)
                
                # try to connect to database and execute sql command to obtain information
                # of nodes match table name
                try:
                    dbconn = sqlite3.connect(dbfile)
                    cur = dbconn.cursor()
                    cur.execute("SELECT tname, nodedriver, nodeurl, nodeid FROM DTABLES WHERE tname = '{0}';".format(tablename))
                    nodelist = cur.fetchall()
                    
                    self.csocket.send(pickle.dumps(nodelist))
                    print(nodelist)
                
                # send fail message if error
                except sqlite3.Error:
                    self.csocket.send(pickle.dumps("Fail"))
                
                # clean up
                finally:
                    cur.close()
                    dbconn.commit()
                    dbconn.close()
        
            # else node server
            else:
                # get database name
                db = c_or_n['database']
                tname = c_or_n['tname']
                csv_size = pickle.loads(self.csocket.recv(1024))

                csvdata = []
                for i in range(csv_size):
                    csvdata.append(pickle.loads(self.csocket.recv(1024)))
                
                # insert all data sent from client
                try:
                    dbconn = sqlite3.connect(db)
                    cur = dbconn.cursor()
                    
                    for data in csvdata:
                        insertState = "INSERT INTO {0} VALUES (".format(tname)
                        for string in data:
                            insertState = insertState + "'" + string + "'" + ", "
                        insertState = insertState[:-2]
                        insertState = insertState + ");"
                        cur.execute(insertState)
                except sqlite3.Error:
                    print("failed")
                finally:
                    cur.close()
                    dbconn.commit()
                    dbconn.close()
    
        # check if column exit
        if r_or_l == "checkColumn":
            tname = pickle.loads(self.csocket.recv(1024))
            print(tname)
            database = pickle.loads(self.csocket.recv(1024))
            print(database)
            colname = pickle.loads(self.csocket.recv(1024))
            print(colname)
            
            # try get all column name and see if partition column exist
            try:
                dbconn = sqlite3.connect(database)
                cur = dbconn.cursor()
                cur.execute("pragma table_info('{0}');".format(tname))
                colinfo = cur.fetchall()
                
                colnum = -1
                
                for row in colinfo:
                    if row[1] == colname:
                        colnum = row[0]
            
            
                self.csocket.send(pickle.dumps(colnum))
                print(colnum)
                
            # send fail message if error
            except sqlite3.Error:
                self.csocket.send(pickle.dumps("Fail"))
                
            # clean up
            finally:
                cur.close()
                dbconn.commit()
                dbconn.close()

        # update catalog
        if r_or_l == "updateCatalog":
            catalogInfo = pickle.loads(self.csocket.recv(1024))
            partInfo = pickle.loads(self.csocket.recv(1024))

            try:
                dbconn = sqlite3.connect(catalogInfo['database'])
                cur = dbconn.cursor()

                # base on partition method
                if partInfo['partmtd'] == 'notpartition':
                    cur.execute("UPDATE DTABLES SET partmtd = 0 WHERE tname = '{0}';".format(catalogInfo['tablename']))
                elif partInfo['partmtd'] == 'hash':
                    cur.execute("UPDATE DTABLES SET partmtd = 2, partcol = '{0}', partparam1 = '{1}' WHERE tname = '{2}';".format(partInfo['partcol'], partInfo['partparam1'], catalogInfo['tablename']))
                elif partInfo['partmtd'] == 'range':
                    for i in range(int(partInfo['numnodes'])):
                        cur.execute("UPDATE DTABLES SET partmtd = 1, partcol = '{0}', partparam1 = '{1}', partparam2 = '{2}' WHERE tname = '{3}' AND nodeid = {4};".format(partInfo['partcol'], partInfo['node' + str(i+1)]['partparam1'], partInfo['node' + str(i+1)]['partparam2'], catalogInfo['tablename'], i + 1))

                self.csocket.send(pickle.dumps("Success"))
            except sqlite3.Error:
                self.csocket.send(pickle.dumps("Failed"))
                        
            finally:
                cur.close()
                dbconn.commit()
                dbconn.close()

        # close connection
        self.csocket.close()    
        print("Connection close " + self.ip + " " + str(self.port))

if __name__ == '__main__':
    # Check for correct number of arguments
    if len(sys.argv) != 3:
        print("Invalid command line argument")
        print("Usage: python3 parDBd.py <hostname> <port#>")
        sys.exit()

    host = sys.argv[1]

    # Check for correct port number input
    try:
        port = int(sys.argv[2])
    except ValueError:
        print("Invalid port number: should be an integer")
        sys.exit()

    if port > 65535 or port < 1024:
        print("Invalid port number: should be between 1024 to 65535")
        print("port # 0 - 1024 are reserved for privilleged service")
        sys.exit()

    # Create socket object
    mySocket = socket.socket()
    
    # Check if able to bind to the server and port
    try:
        mySocket.bind((host, port))
    except socket.error:
        print("Unable to bind")
        sys.exit()
    
    # Listening for connection
    mySocket.listen(5)
    print("Server is listening")
    
    # Accept new client connection and start thread
    while True:
        (conn, (ip, port)) = mySocket.accept()
        newThread = ListenThread(ip, port, conn)
        newThread.start()
    
    # close server socket
    mySocket.close()


