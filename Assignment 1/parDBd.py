##########################################################################################
# 
# Name: Hongbo Lin
# Class: ICS 421 Database System II
# Professor: Lipyeow Lim
# Date: Janauary 25, 2018
# Assignment: #1 (for detail, please see 
#                 https://lipyeow.github.io/ics421s18/morea/pardb/experience-hw1.html)
# File: parDBd.py
# Description: server program
#
##########################################################################################

####################################################################
# NOTES: The program runs on Python 3 and uses sqlite3 as database #
####################################################################

# Import all necessary library
import threading
import sqlite3
import socket
import sys

# Thread class for server to accept multiple connection from clients
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
        print("New Thread started for " + ip + " " + str(port))

    def run(self):
        '''
            Define the run function for the ListenThread
            if the server is node
                recv the database name and sql command from client, try connect to the
                database and execute the sql command and send back the message whether
                command success or failed.
            if the server is catalog
                recv the database name and open db connection, then create dtable if not
                exist. Receive the number of information update. Loop to receive all info
                about each update and execute sql insert or delete base on the info, then
                send back message if success or fail.
        '''
        c_or_n = self.csocket.recv(1024).decode()
        if c_or_n.find("nodeid") >= 0:
            # receive the dababase file name
            dbfile = self.csocket.recv(1024).decode()
        
            # receive the sql command for create or drop table
            sqlcmd = self.csocket.recv(1024).decode()
        
            # try connect to the database file and execute the sql command
            try:
                dbconn = sqlite3.connect(dbfile)
                cur = dbconn.cursor()
                cur.execute(sqlcmd)
                self.csocket.send("Success".encode())
            
            # send fail message if error
            except sqlite3.Error:
                self.csocket.send("Fail".encode())
            
            # clean up
            finally:
                cur.close()
                dbconn.commit()
                dbconn.close()
        else:
            
            # receive dbfile name
            dbfile = self.csocket.recv(1024).decode()
        
            try:
                # connect to database
                dbconn = sqlite3.connect(dbfile)
                cur = dbconn.cursor()

                # create dtable if not exists
                try:
                    cur.execute("CREATE TABLE DTABLE(tname char(32), nodedriver char(64), nodeurl char(128), nodeuser char(16), nodepasswd char(16), partmtd int, nodeid int, partparam1 char(32), partparam2 char(32));")
                except:
                    pass
            
                # num of update need to execute
                numUpdate = int(self.csocket.recv(1024).decode())
                
                # loop receive info and base on it, either insert or delete
                for i in range(numUpdate):
                    temp = self.csocket.recv(1024).decode().split(" ")
                    if temp[0] == "CREATE":
                        cur.execute("INSERT INTO DTABLE(tname, nodedriver, nodeurl, nodeuser, nodepasswd, partmtd, nodeid, partparam1, partparam2) VALUES ('{0}', '{1}', NULL, NULL, NULL, NULL, '{2}', NULL, NULL);".format(temp[1], temp[2], int(temp[3])))
                    elif temp[0] == "DROP":
                        cur.execute("DELETE FROM DTABLE WHERE tname = '{0}' AND nodeid = '{1}'".format(temp[1], int(temp[3])))
        
                # send updated message if no error
                self.csocket.send("Updated".encode())
                    
            # send fail message if error occurs
            except sqlite3.Error:
                self.csocket.send("Fail".encode())
            
            # clean up
            finally:
                cur.close()
                dbconn.commit()
                dbconn.close()
        
        # close connection
        self.csocket.close()


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
