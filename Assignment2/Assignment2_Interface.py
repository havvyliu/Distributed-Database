#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    #Implement RangeQuery Here.
    cur = openconnection.cursor()


    Rangename = "RangeRatingsPart"
    RRname = "RoundRobinRatingsPart"


    f = open("RangeQueryOut.txt", "w")


    # going through round robin partition
    hasNext = True
    ptrSize = 0
    while hasNext:
        cur.execute("select EXISTS (select * from information_schema.tables where table_name='roundrobinratingspart" + str(
            ptrSize) + "');")
        data = cur.fetchall()
        if data[0][0]:
            cur.execute("SELECT * FROM {0}{1};".format(RRname, ptrSize))
            data = cur.fetchall()
            for entry in data:
                if ratingMinValue <= float(entry[2]) <= ratingMaxValue:
                    f.write("{0}{1},{2},{3},{4}\n".format(RRname, ptrSize, entry[0], entry[1], entry[2]))
            ptrSize += 1
        else:
            hasNext = False

    # going through range partition
    ptrSize = 0
    hasNext = True
    while hasNext:
        # cur.execute("select * from information_schema.tables where table_name="+"'"+Rangename+"1';")

        cur.execute("select EXISTS (select * from information_schema.tables  where table_name='rangeratingspart{0}');".format(ptrSize))
        data = cur.fetchall()
        if data[0][0]:
            cur.execute("SELECT * FROM {0}{1};".format(Rangename, ptrSize))
            data = cur.fetchall()
            for entry in data:
                if ratingMinValue <= float(entry[2]) <= ratingMaxValue:
                    f.write("{0}{1},{2},{3},{4}\n".format(Rangename, ptrSize, entry[0], entry[1], entry[2]))
            ptrSize += 1
        else:
            hasNext = False

    f.close()


def PointQuery(ratingsTableName, ratingValue, openconnection):
    #Implement PointQuery Here.
    cur = openconnection.cursor()

    Rangename = "RangeRatingsPart"
    RRname = "RoundRobinRatingsPart"

    f = open("PointQueryOut.txt", "w")



    # going through round robin partition
    hasNext = True
    ptrSize = 0
    while hasNext:
        cur.execute(
            "select EXISTS (select * from information_schema.tables where table_name='roundrobinratingspart" + str(
                ptrSize) + "');")
        data = cur.fetchall()
        if data[0][0]:
            cur.execute("SELECT * FROM {0}{1};".format(RRname, ptrSize))
            data = cur.fetchall()
            for entry in data:
                if float(entry[2]) == ratingValue:
                    f.write("{0}{1},{2},{3},{4}\n".format(RRname, ptrSize, entry[0], entry[1], entry[2]))
            ptrSize += 1
        else:
            hasNext = False


    # going through range partition
    ptrSize = 0
    hasNext = True

    while hasNext:
        # cur.execute("select * from information_schema.tables where table_name="+"'"+Rangename+"1';")

        cur.execute(
            "select EXISTS (select * from information_schema.tables  where table_name='rangeratingspart{0}');".format(
                ptrSize))
        data = cur.fetchall()
        if data[0][0]:
            cur.execute("SELECT * FROM {0}{1};".format(Rangename, ptrSize))
            data = cur.fetchall()
            for entry in data:
                if float(entry[2]) == ratingValue:
                    f.write("{0}{1},{2},{3},{4}\n".format(Rangename, ptrSize, entry[0], entry[1], entry[2]))
            ptrSize += 1
        else:
            hasNext = False


    f.close()
