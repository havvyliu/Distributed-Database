#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2
import pandas as pd
from sqlalchemy import create_engine

DATABASE_NAME = 'dds_assgn1'

count = 0
rr_partition = 0
range_partition = 0


def getopenconnection(user='postgres', password='1234', dbname='dds_assgn1'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def test(openconnection):
    # sql to be tested
    sql =  "SELECT * FROM rrobin_part0;"
    cur = openconnection.cursor()
    cur.execute(sql)
    query = cur.fetchall()

    print(query)

def loadratings(ratingstablename, ratingsfilepath, openconnection):
    """Using to_sql doesnt seem to work, swtich to old style"""
    # engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/dds_assgn1')
    # load_data().to_sql(name=ratingstablename, con=engine, if_exists='replace', index=False)

    # clean table
    create_table(openconnection)

    cur = openconnection.cursor();
    for index, row in load_data().iterrows():
        sql = "INSERT INTO RATINGS (UserID, MovieID, Rating) VALUES (%s, %s, %s);"
        cur.execute(sql, (row['userid'], row['movieid'], row['rating']))


    # # clean up
    # cur.close()
    # openconnection.close()


def loadratingsWithOutClean(ratingstablename, ratingsfilepath, openconnection):
    """Using to_sql doesnt seem to work, swtich to old style"""
    # engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/dds_assgn1')
    # load_data().to_sql(name=ratingstablename, con=engine, if_exists='replace', index=False)

    # clean table
    # create_table(openconnection)

    cur = openconnection.cursor();
    for index, row in load_data().iterrows():
        sql = "INSERT INTO RATINGS (UserID, MovieID, Rating) VALUES (%s, %s, %s);"
        cur.execute(sql, (row['userid'], row['movieid'], row['rating']))

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    global  range_partition
    range_partition = max(range_partition, numberofpartitions)

    # get cursor
    cur = openconnection.cursor()

    # clean?
    create_table(openconnection)

    range1 = 5.0 / numberofpartitions

    for num in range(0, numberofpartitions):
        if num==0:
            cur.execute('DROP TABLE IF EXISTS range_part{0}'.format(num))
            # sql = "CREATE TABLE PARTITION_%s (CHECK (rating < %d AND rating >= %d)) INHERITS (RATINGS);"
            cur.execute("CREATE TABLE range_part{0} (CHECK (rating <= {1} AND rating >= {2})) INHERITS (RATINGS);"
                        "CREATE INDEX range_part{3}_idx ON range_part{4} (rating);".format(num, (num+1)*range1, (num)*(range1), num, num))

        else:
            cur.execute('DROP TABLE IF EXISTS range_part{0}'.format(num))
            # sql = "CREATE TABLE PARTITION_%s (CHECK (rating < %d AND rating >= %d)) INHERITS (RATINGS);"
            cur.execute("CREATE TABLE range_part{0} (CHECK (rating <= {1} AND rating > {2})) INHERITS (RATINGS);"
                        "CREATE INDEX range_part{3}_idx ON range_part{4} (rating);".format(num, (num + 1) * range1,
                                                                                           (num) * (range1), num, num))


    # create create_partition_insert()
    if numberofpartitions <= 1:
        sql_function = "" \
                       "CREATE OR REPLACE FUNCTION create_partition_insert() " \
                       "RETURNS trigger AS $$" \
                        "BEGIN " \
                       "IF (NEW.rating < {0} AND NEW.rating >= {1}) " \
                            "THEN INSERT INTO range_part{2} VALUES (NEW.*);" \
                       "ELSE " \
                            "RAISE EXCEPTION 'Date out of range. check orders_insert() function!';" \
                       "END IF;" \
                       "RETURN NULL;" \
                       "END; " \
                       "$$ " \
                       "LANGUAGE plpgsql;"\
            .format(6, 0, 0)
        cur.execute(sql_function)
    else:
        sql = ""
        for num in range(1, numberofpartitions):
            sql_mid =     "ELSIF (NEW.rating > {0} AND NEW.rating <= {1}) " \
                                "THEN INSERT INTO range_part{2} VALUES (NEW.*);".format(range1*num, range1*(num+1), num)
            sql = sql + sql_mid
        sql_begin = "CREATE OR REPLACE FUNCTION create_partition_insert() " \
                       "RETURNS TRIGGER AS " \
                        "$$ " \
                        "BEGIN " \
                       "IF (NEW.rating <= {0} AND NEW.rating >= {1}) " \
                            "THEN INSERT INTO range_part{2} VALUES (NEW.*);".format(range1, 0, 0)
        sql_end =       "ELSE " \
                            "RAISE EXCEPTION 'Date out of range. check orders_insert() function!';" \
                        "END IF;" \
                        "RETURN NULL;" \
                       "END; " \
                       "$$ " \
                       "LANGUAGE plpgsql;"
        cur.execute(sql_begin + sql + sql_end)

    # create trigger
    trigger_sql = "DROP TRIGGER IF EXISTS partition_insert_trigger ON RATINGS;" \
                  "CREATE TRIGGER create_insert_trigger" \
                  "  BEFORE INSERT ON RATINGS" \
                  "   FOR EACH ROW EXECUTE PROCEDURE create_partition_insert();"
    cur.execute(trigger_sql)


    loadratingsWithOutClean('RATINGS', 'ml-10M100K/ratings.dat', openconnection)


    openconnection.commit()



def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    global rr_partition
    rr_partition = max(rr_partition, numberofpartitions)

    # get cursor
    cur = openconnection.cursor()

    delete_rrobin_part_table(rr_partition, openconnection)

    # partition table creation
    for num in range(0, numberofpartitions):
        cur.execute('DROP TABLE IF EXISTS rrobin_part{0}'.format(num))
        # sql = "CREATE TABLE PARTITION_%s (CHECK (rating < %d AND rating >= %d)) INHERITS (RATINGS);"
        cur.execute("CREATE TABLE rrobin_part{0} (CHECK (rating <= 5 AND rating >= 0)) INHERITS (RATINGS);".format(num))

    # get count
    global count
    count = 0

    global rr_partition
    rr_partition = numberofpartitions


    # insert data into partition table
    cur.execute("SELECT * FROM RATINGS")
    data = cur.fetchall()
    print data
    for row in data:
        cur.execute("INSERT INTO rrobin_part{0}(UserID, MovieID, Rating) VALUES ({1},{2},{3})".format(count, row[1], row[2], row[3]))
        count += 1
        count = count % numberofpartitions

    # create trigger
    # trigger_sql = "DROP TRIGGER IF EXISTS create_rr_insert_trigger ON RATINGS;" \
    #               "CREATE TRIGGER create_rr_insert_trigger" \
    #               "  BEFORE INSERT ON RATINGS" \
    #               "   FOR EACH ROW EXECUTE PROCEDURE create_rr_partition_insert();"
    # cur.execute(trigger_sql)

    openconnection.commit()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    # insert data
    cur = openconnection.cursor()
    cur.execute("INSERT INTO {0}(userid, movieid, rating) VALUES ({1}, {2}, {3})".format(ratingstablename, userid, itemid, rating))
    cur.execute("INSERT INTO rrobin_part{0}(userid, movieid, rating) VALUES ({1}, {2}, {3})".format(count, userid, itemid, rating))
    global count
    count += 1
    count = count % rr_partition

    # test


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    # since we have trigger just insert directly
    cur = openconnection.cursor()
    cur.execute("INSERT INTO RATINGS(userid, movieid, rating) VALUES ({0},{1},{2})".format(userid, itemid, rating))

    openconnection.commit()


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
    sql_table_create = 'DROP TABLE IF EXISTS RATINGS CASCADE ;' \
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
    cur.close()

"""Load data into dataframe using Pandas and return it"""
def load_data():
    ratingcolumns = ['userid', 'movieid', 'rating', 'time']
    dataFrame = pd.read_csv('ml-10M100K/ratings.dat',delimiter="::", names=ratingcolumns)
    dataFrame.drop(labels='time', axis=1, inplace=True)
    return dataFrame

def deletepartitionsandexit(openconnection):
    delete_range_part_table(range_partition, openconnection)
    delete_rrobin_part_table(rr_partition, openconnection)
    create_table(openconnection)
    exit(code=100)

# delete all main table as well as other partition table
def delete_rrobin_part_table(numberofpartition, openconnection):
    cur = openconnection.cursor()
    # delete range_part
    for num in range(0, numberofpartition):
        if num==0:
            cur.execute('DROP TABLE IF EXISTS rrobin_part{0}'.format(num))


def delete_range_part_table(numberofpartition, openconnection):
    cur = openconnection.cursor()
    # delete range_part
    for num in range(0, numberofpartition):
        if num == 0:
            cur.execute('DROP TABLE IF EXISTS range_part{0}'.format(num))

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
    create_table(openconnection)
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
            #
            #
            roundrobinpartition(table_name, 8, con)
            #
            #
            # # roundrobininsert(table_name,3, 3,5, con)

            #
            create_table(con)
            deletepartitionsandexit(con)


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
