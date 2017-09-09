import psycopg2
import pandas as pd
from configparser import ConfigParser
import config

def readData():
    movieColumnsInfo = ["index", "title", "Category"]
    moviedataframe = pd.read_csv("./ml-10M100K/movies.dat",names=movieColumnsInfo, delimiter="::")

    ratingsColumnInfo = ["UserID", "MovieID", "Rating", "LogIndex"]
    ratingsDataFrame = pd.read_csv("./ml-10M100K/ratings.dat",names=ratingsColumnInfo, delimiter="::")



def config(filename='database.ini', section='postgresql'):
    parser = ConfigParser()
    parser.read(filename)

    db={}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file')

    return db

def connect():
    conn = None
    try:
        params = config()

        print('Connecting to database..')
        conn = psycopg2.connect(**params)
        # Create a cursor
        cur = conn.cursor()

        """sql to be executed"""
        sql = 'CREATE TABLE IF NOT EXISTS RATINGS(' \
              'UserID INT PRIMARY KEY NOT NULL,' \
              'MovieID INT NOT NULL,' \
              'Rating INT NOT NULL' \
                ')';


        print('PostgreSQL database :')
        cur.execute(sql)
        db_query = cur.fetchone()
        print(db_query)

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connect closed')

if __name__ == '__main__':
    connect()