#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import threading, Queue

##################### This needs to changed based on what kind of table we want to sort. ##################
##################### To know how to change this, see Assignment 3 Instructions carefully #################
FIRST_TABLE_NAME = 'ratings'
SECOND_TABLE_NAME = 'movies'
SORT_COLUMN_NAME_FIRST_TABLE = 'MovieId1'
SORT_COLUMN_NAME_SECOND_TABLE = 'column2'
JOIN_COLUMN_NAME_FIRST_TABLE = 'movieid'
JOIN_COLUMN_NAME_SECOND_TABLE = 'MovieId1'
##########################################################################################################




# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT * FROM {0};".format(InputTable))
    data = cur.fetchall()
    q = Queue.Queue()
    for entry in data:
        q.put(entry)

    cur = openconnection.cursor()
    #create output table
    cur.execute("CREATE TABLE IF NOT EXISTS {0} AS TABLE {1} WITH NO DATA;".format(OutputTable, InputTable))

    #create partitiontable
    for i in range(5):
        cur.execute("CREATE TABLE psortpart{0} AS TABLE {1} WITH NO DATA;".format(i,InputTable))

    cur.execute("SELECT MAX({0}) FROM {1};".format(SortingColumnName,InputTable))
    maxValue = cur.fetchone()[0]

    cur.execute("SELECT MIN({0}) FROM {1};".format(SortingColumnName, InputTable))
    minValue = cur.fetchone()[0]

    # start the sorting processes
    t1 = threading.Thread(target=ParallelSortPart,
                            args=(InputTable, SortingColumnName, OutputTable, openconnection, q, maxValue, minValue))
    t2 = threading.Thread(target=ParallelSortPart,
                            args=(InputTable, SortingColumnName, OutputTable, openconnection, q, maxValue, minValue))
    t3 = threading.Thread(target=ParallelSortPart,
                            args=(InputTable, SortingColumnName, OutputTable, openconnection, q, maxValue, minValue))
    t4 = threading.Thread(target=ParallelSortPart,
                            args=(InputTable, SortingColumnName, OutputTable, openconnection, q, maxValue, minValue))
    t5 = threading.Thread(target=ParallelSortPart,
                            args=(InputTable, SortingColumnName, OutputTable, openconnection, q, maxValue, minValue))
    t1.start()
    t2.start()
    t3.start()
    t4.start()
    t5.start()

    while t1.is_alive() or t2.is_alive() or t3.is_alive() or t4.is_alive() or t5.is_alive():
        #wait and do nothing really
        # Threading creates new thread that run concurrently with our main thread, in order to get the data that are being processed by other threads, we should
        # wait until all the threads to finish then continue

        pass

    for i in range(5):
        cur.execute("SELECT * FROM psortpart{0}".format(i))
        data = cur.fetchall()
        for entry in data:
            cur.execute("INSERT INTO {0} VALUES ('{1}', '{2}', '{3}')".format(OutputTable, entry[0], entry[1], entry[2]))

    # # TODO delete when done, just for testing
    # cur.execute("SELECT * FROM {0}".format(OutputTable))
    # # cur.execute("SELECT * FROM psortpart2")
    # data = cur.fetchall()
    # print data

def ParallelSortPart (InputTable, SortingColumnName, OutputTable, openconnection, q, maxValue, minValue):
    cur = openconnection.cursor()
    cur.execute("select column_name from information_schema.columns where table_name='{0}';".format(InputTable))
    columndata = cur.fetchall()
    i = 0
    columnindex = -1
    for entry in columndata:
        if str(SortingColumnName).lower() in str(entry).lower():
            columnindex = i
        i += 1
    step = (maxValue - minValue) / 5
    while not q.empty():
        data = q.get()
        if data[columnindex] <= minValue + step:
            cur.execute("INSERT INTO psortpart0 VALUES ('{0}','{1}','{2}')".format(data[0], data[1], data[2]))
        elif minValue + step < data[columnindex] <= minValue + step*2:
            cur.execute("INSERT INTO psortpart1 VALUES ('{0}','{1}','{2}')".format(data[0], data[1], data[2]))
        elif minValue + step*2 < data[columnindex] <= minValue + step*3:
            cur.execute("INSERT INTO psortpart2 VALUES ('{0}','{1}','{2}')".format(data[0], data[1], data[2]))
        elif minValue + step*3 < data[columnindex] <= minValue + step*4:
            cur.execute("INSERT INTO psortpart3 VALUES ('{0}','{1}','{2}')".format(data[0], data[1], data[2]))
        elif minValue + step*4 < data[columnindex] <= minValue + step*5:
            cur.execute("INSERT INTO psortpart4 VALUES ('{0}','{1}','{2}')".format(data[0], data[1], data[2]))
        else:
            pass

def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    cur = openconnection.cursor()

    # Create output table
    cur.execute("select column_name from information_schema.columns where table_name='{0}';".format(InputTable1))
    columnInfoTable1 = cur.fetchall()
    cur.execute("select column_name from information_schema.columns where table_name='{0}';".format(InputTable2))
    columnInfoTable2 = cur.fetchall()
    # print columnInfoTable2[0]
    # print Table2JoinColumn
    cur.execute("CREATE TABLE {0} AS (SELECT * FROM {1}, {2}) WITH NO DATA ".format(OutputTable, InputTable1, InputTable2))
    cur.execute("DELETE FROM {0}".format(OutputTable))
    cur.execute("select column_name from information_schema.columns where table_name='{0}';".format(OutputTable))


    cur.execute("SELECT * FROM {0};".format(InputTable1))
    data = cur.fetchall()
    q = Queue.Queue()
    for entry in data:
        q.put(entry)
    t1 = threading.Thread(target=ParallelJoinPart,
                     args=(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection, q))
    t2 = threading.Thread(target=ParallelJoinPart,
                     args=(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection, q))
    t3 = threading.Thread(target=ParallelJoinPart,
                     args=(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection, q))
    t4 = threading.Thread(target=ParallelJoinPart,
                     args=(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection, q))
    t5 = threading.Thread(target=ParallelJoinPart,
                     args=(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection, q))
    t1.start()
    t2.start()
    t3.start()
    t4.start()
    t5.start()
    while t1.is_alive() or t2.is_alive() or t3.is_alive() or t4.is_alive() or t5.is_alive():
        #wait and do nothing really
        # Threading creates new thread that run concurrently with our main thread, in order to get the data that are being processed by other threads, we should
        # wait until all the threads to finish then continue
        pass
    # TODO delete when done, just for testing
    # cur.execute("SELECT * FROM {0}".format(OutputTable))
    # # cur.execute("SELECT * FROM psortpart2")
    # data = cur.fetchall()
    # print data

def ParallelJoinPart (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection, q):
    cur = openconnection.cursor()
    cur.execute("select column_name from information_schema.columns where table_name='{0}';".format(InputTable1))
    columnInfoTable1 = cur.fetchall()
    cur.execute("select column_name from information_schema.columns where table_name='{0}';".format(InputTable2))
    columnInfoTable2 = cur.fetchall()
    i = 0
    columnindex2 = -1
    columnindex1 = -1
    for entry in columnInfoTable1:
        if str(Table1JoinColumn).lower() in str(entry).lower():
            columnindex1 = i
        i += 1
    i = 0
    for entry in columnInfoTable2:
        if str(Table2JoinColumn).lower() in str(entry).lower():
            columnindex2 = i
        i += 1
    cur.execute("SELECT * FROM {0}".format(InputTable2))
    tabledata2 = cur.fetchall()
    while not q.empty():
        data = q.get()
        for entry in tabledata2:
            if entry[columnindex2] == data[columnindex1]:
                cur.execute("INSERT INTO {0} VALUES ({1}, {2}, {3}, {4}, '{5}', '{6}')".format(OutputTable, data[0], data[1],
                                                                                           data[2], entry[0], entry[1],
                                                                                           entry[2]))


################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.commit()
    con.close()



# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" %(ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d`+",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

if __name__ == '__main__':
    try:
	# Creating Database ddsassignment3
	print "Creating Database named as ddsassignment3"
	createDB();
	
	# Getting connection to the database
	print "Getting connection from the ddsassignment3 database"
	con = getOpenConnection();


	# Calling ParallelSort
	print "Performing Parallel Sort"
	ParallelSort(FIRST_TABLE_NAME, SORT_COLUMN_NAME_FIRST_TABLE, 'parallelSortOutputTable', con);

	# Calling ParallelJoin
	print "Performing Parallel Join"
	ParallelJoin(FIRST_TABLE_NAME, SECOND_TABLE_NAME, JOIN_COLUMN_NAME_FIRST_TABLE, JOIN_COLUMN_NAME_SECOND_TABLE, 'parallelJoinOutputTable', con);
	
	# Saving parallelSortOutputTable and parallelJoinOutputTable on two files
	saveTable('parallelSortOutputTable', 'parallelSortOutputTable.txt', con);
	saveTable('parallelJoinOutputTable', 'parallelJoinOutputTable.txt', con);

	# Deleting parallelSortOutputTable and parallelJoinOutputTable
	deleteTables('parallelSortOutputTable', con);
        deleteTables('parallelJoinOutputTable', con);

        if con:
            con.close()

    except Exception as detail:
        print "Something bad has happened!!! This is the error ==> ", detail
