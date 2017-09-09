#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2
import pandas as pd
from sqlalchemy import create_engine

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='1234', dbname='dds_assgn1'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def test(openconnection):
    # sql to be tested
    sql =  "SELECT * FROM RATINGS;"
    cur = openconnection.cursor()
    cur.execute(sql)
    query = cur.fetchall()

    print(query)

def loadratings(ratingstablename, ratingsfilepath, openconnection):
    """Using to_sql doesnt seem to work, swtich to old style"""
    # engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/dds_assgn1')
    # load_data().to_sql(name=ratingstablename, con=engine, if_exists='replace', index=False)
    cur = openconnection.cursor();
    for index, row in load_data().iterrows():
        sql = "INSERT INTO RATINGS (UserID, MovieID, Rating) VALUES (%s, %s, %s);"
        cur.execute(sql, (row['userid'], row['movieid'], row['rating']))

    openconnection.commit()
    test(openconnection=openconnection)

    # clean up
    cur.close()
    openconnection.close()


def rangepartition(ratingstablename, numberofpartitions, openconnection):
    pass


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    pass


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    pass


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    pass


def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='dds_assgn1')
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
    con.close()

def create_table(openconnection):
    """sql to be executed to generate table"""
    sql_table_create = 'DROP TABLE RATINGS;' \
                       'CREATE TABLE RATINGS(' \
          'Index SERIAL PRIMARY KEY,' \
            'UserID INT NOT NULL,' \
          'MovieID INT NOT NULL,' \
          'Rating INT NOT NULL' \
          ')';
    """create cursor"""
    cur = openconnection.cursor()
    """generate new table"""
    cur.execute(sql_table_create)
    print("table has been generated")
    openconnection.commit()
    cur.close()

"""Load data into dataframe using Pandas and return it"""
def load_data():
    ratingcolumns = ['userid', 'movieid', 'rating', 'time']
    dataFrame = pd.read_csv('ml-10M100K/ratings.dat',delimiter="::", names=ratingcolumns)
    dataFrame.drop(labels='time', axis=1, inplace=True)
    return dataFrame


# Middleware
def before_db_creation_middleware():
    # create table needed
    create_table(getopenconnection())

    # load data into dataframe using pandas
    load_data()


def after_db_creation_middleware(databasename):
    # Use it if you want to
    pass


def before_test_script_starts_middleware(openconnection, databasename):
    # Use it if you want to
    pass


def after_test_script_ends_middleware(openconnection, databasename):
    # Use it if you want to
    pass


if __name__ == '__main__':
    try:

        # Use this function to do any set up before creating the DB, if any
        before_db_creation_middleware()

        create_db(DATABASE_NAME)

        # Use this function to do any set up after creating the DB, if any
        after_db_creation_middleware(DATABASE_NAME)

        with getopenconnection() as con:
            # Use this function to do any set up before I starting calling your functions to test, if you want to
            before_test_script_starts_middleware(con, DATABASE_NAME)

            #set table name to RATINGS
            table_name = 'RATINGS'

            #set filepath
            filepath = '/ml-10M100K/ratings.dat'

            loadratings(table_name, filepath, con)


            # Here is where I will start calling your functions to test them. For example,
            # loadratings('ratings.dat', con)
            # ###################################################################################
            # Anything in this area will not be executed as I will call your functions directly
            # so please add whatever code you want to add in main, in the middleware functions provided "only"
            # ###################################################################################

            # Use this function to do any set up after I finish testing, if you want to
            after_test_script_ends_middleware(con, DATABASE_NAME)

    except Exception as detail:
        print "OOPS! This is the error ==> ", detail
