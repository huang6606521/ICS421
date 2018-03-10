##########################################################################################
# 
# Name: Hongbo Lin
# Class: ICS 421 Database System II
# Professor: Lipyeow Lim
# Date: February 15, 2018
# Assignment: #2 (for detail, please see 
#                https://lipyeow.github.io/ics421s18/morea/queryproc/experience-hw2.html)
# File: loadCSV.py
# Desciption:
#
##########################################################################################

####################################################################
# NOTES: The program runs on Python 3 and uses sqlite3 as database #
####################################################################

# import all necessary library
import threading
import pickle
import socket
import time
import sys
import csv
import re

def readCFG(clustercfg):
    '''
        Open the clustercfg file and parse info of catalog DB
        
        Parameter:
            clustercfg: The clustercfg file to be parsed

        Return:
            catalog: a dict that contains information of catalog DB
            partitionInfo: a dict that contains information of partitioni
    '''
    # try open clustercfg file and parse infomation of catalog DB and partition info
    try:
        with open(clustercfg) as cluster:
            partitionInfo = dict()

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
                    elif "tablename" in temp[0]:
                        tablename = temp[1].replace("\n", "")
                    elif "numnodes" in temp[0]:
                        numnodes = temp[1].replace("\n", "")
                        partitionInfo['numnodes'] = numnodes
                    elif "method" in temp[0]:
                        partmtd = temp[1].replace("\n", "")
                        partitionInfo['partmtd'] = partmtd
                    elif "column" in temp[0]:
                        partcol = temp[1].replace("\n", "") 
                        partitionInfo['partcol'] = partcol
                    elif "partition.param1" in temp[0]:
                        partitionInfo['partparam1'] = temp[1].replace("\n", "")
                    elif "node" in temp[0]:
                        if "param1" in temp[0]:
                            partparam1 = temp[1].replace("\n", "")
                        if "param2" in temp[0]:
                            nodeid = temp[0].split(".")[1]
                            param12 = {
                                'partparam1': partparam1,
                                'partparam2': temp[1].replace("\n", ""),
                            }
                            partitionInfo[nodeid] = param12

            # catalog dict contain all information about catalog DB
            catalog = {
                'driver': driver,
                'host': host,
                'port': port,
                'database': database,
                'username': username,
                'passwd': passwd,
                'tablename': tablename,
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
    return catalog, partitionInfo

def loaderConnection(node, csvdata):
    '''
        connect the node server and insert csvdata into the database
    '''
    try:
        nodeSocket = socket.socket()
        nodeSocket.connect((node['host'], int(node['port'])))

        nodeSocket.send(pickle.dumps("loadCSV.py"))
        time.sleep(0.1)
        nodeSocket.send(pickle.dumps(node))
        time.sleep(0.1)
        nodeSocket.send(pickle.dumps(len(csvdata)))
        time.sleep(0.1)
        for data in csvdata:
            nodeSocket.send(pickle.dumps(data))
            time.sleep(0.1)
    except socket.error:
        print("Something wrong")
    finally:
        nodeSocket.close() 

# The main program
if __name__ == '__main__':

    # check if correct number argument are passed in
    if len(sys.argv) != 3:
        print("Invalid command line arguments")
        print("Usage: python3 loadCSV.py <clustercfg> <csvfile>")
        print("       <clustercfg>: contains information of catalog DB and partition method")
        print("       <csvfile>: contain all data to be loaded")
        sys.exit()

    clustercfg = sys.argv[1]
    csvfile = sys.argv[2]

    catalog, part = readCFG(clustercfg)

    # print(catalog)
    # print(part)

    # try connect to catalog database to get nodes info
    try:
        # connect to catalog
        catalogSocket = socket.socket()
        catalogSocket.connect((catalog['host'], catalog['port']))

        # send necessary stuffs to catalog server
        catalogSocket.send(pickle.dumps("loadCSV.py"))
        time.sleep(0.1)
        catalogSocket.send(pickle.dumps(catalog))
        
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

    # check partition method

    # if notpartition, then insert all data into all node
    if part['partmtd'] == 'notpartition':

        rows = []
        with open(csvfile) as csvcontent:
            reader = csv.reader(csvcontent)
            for row in reader:
                rows.append(row)

        for node in nodes:
            loaderConnection(node, rows)
            print("[{0}://{1}:{2}/{3}]: {4} rows inserted.".format(node['driver'], node['host'], node['port'], node['database'], len(rows)))

    else:
        # check partition column exist
        try:
            checkSocket = socket.socket()
            checkSocket.connect((nodes[0]['host'], int(nodes[0]['port'])))

            checkSocket.send(pickle.dumps("checkColumn"))
            time.sleep(0.1)
            checkSocket.send(pickle.dumps(nodes[0]['tname']))
            time.sleep(0.1)
            checkSocket.send(pickle.dumps(nodes[0]['database']))
            time.sleep(0.1)
            checkSocket.send(pickle.dumps(part['partcol']))
  
            colnum = pickle.loads(checkSocket.recv(1024))

            if colnum == -1:
                print("Partition column name does not exist")
                sys.exit()

        except socket.error:
            print("Something went wrong")
            sys.exit()

        finally:
            checkSocket.close()

        # print(colnum)

        # if hash, insert data base on X = (parcol mod param1) + 1
        if part['partmtd'] == 'hash':
            if num != int(part['partparam1']):
                print("Number of nodes and number of partition does not match")
                sys.exit()
            csv_data = []
            for i in range(num):
                data = []
                csv_data.append(data)

            with open(csvfile) as csvcontent:
                reader = csv.reader(csvcontent)
                for row in reader:
                    X = (int(row[int(colnum)]) % int(part['partparam1'])) + 1

                    csv_data[X - 1].append(row)

            for i in range(num):
                loaderConnection(nodes[i], csv_data[i])
                print("[{0}://{1}:{2}/{3}]: {4} rows inserted.".format(nodes[i]['driver'], nodes[i]['host'], nodes[i]['port'], nodes[i]['database'], len(csv_data[i])))

        # if range, partparam1 < partcol <= partparam2
        elif part['partmtd'] == 'range':
            if num != int(part['numnodes']):
                print("Number of nodes and number of partition does not match")
                sys.exit()
            csv_data = []
            for i in range(num):
                data = []
                csv_data.append(data)

            param12 = []
            for i in range(num):
                param12.append([part['node' + str(i + 1)]['partparam1'], part['node' + str(i + 1)]['partparam2']])

            print(param12)

            with open(csvfile) as csvcontent:
                reader = csv.reader(csvcontent)
                for row in reader:
                    for i in range(num):
                        if param12[i][0] == '-inf':
                            if int(row[int(colnum)]) <= int(param12[i][1]):
                                csv_data[i].append(row)
                        elif param12[i][1] == '+inf':
                            if int(row[int(colnum)]) > int(param12[i][0]):
                                csv_data[i].append(row)
                        else:
                            if int(row[int(colnum)]) <= int(param12[i][1]) and int(row[int(colnum)]) > int(param12[i][0]):
                                csv_data[i].appen(row)
            print(csv_data)
            for i in range(num):
                loaderConnection(nodes[i], csv_data[i])
                print("[{0}://{1}:{2}/{3}]: {4} rows inserted.".format(nodes[i]['driver'], nodes[i]['host'], nodes[i]['port'], nodes[i]['database'], len(csv_data[i])))

    # update catalog with partition information
    try:
        # connect to catalog
        catalogSocket = socket.socket()
        catalogSocket.connect((catalog['host'], catalog['port']))

        # send necessary stuffs to catalog server
        catalogSocket.send(pickle.dumps("updateCatalog"))
        time.sleep(0.1)
        catalogSocket.send(pickle.dumps(catalog))
        time.sleep(0.1)
        catalogSocket.send(pickle.dumps(part))

        message = pickle.loads(catalogSocket.recv(1024))

        if message == "Success":
            print("[{0}://{1}:{2}/{3}]: catalog updated.".format(catalog['driver'], catalog['host'], catalog['port'], catalog['database']))
        if message == "Failed":
            print("[{0}://{1}:{2}/{3}]: catalog failed update.".format(catalog['driver'], catalog['host'], catalog['port'], catalog['database']))

    # handle exception
    except socket.error as e:
        print("Something went wrong during connection to catalog database.")
        print(e)
        sys.exit()

    # clean up
    finally:
        catalogSocket.close()

