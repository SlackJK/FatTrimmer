import pandas as pd
import pyodbc
from time import time


# SQL setup
SQL_SERVER_IP = ""
SQL_SERVER_PORT = "1433"
SQL_SERVER_DATABASE = "bazos"
SQL_SERVER_USERID = ""
SQL_SERVER_PASSWORD = ""

def setup_sql():
    """Setup function that establishes connection to a remote database
    Logs into the Microsoft SQL database using the provided username and password
    The con object is responsible to maintaining communication with the server
    The cur object is the cursor with which queries are passed
    """
    global con, cur
    con = pyodbc.connect(
        "Driver={SQL Server Native Client 11.0};"
        f"Server={SQL_SERVER_IP},{SQL_SERVER_PORT};"
        f"Database={SQL_SERVER_DATABASE};"
        f"UID={SQL_SERVER_USERID};"
        f"PWD={SQL_SERVER_PASSWORD}"
    )
    cur = con.cursor()





def get_page_data():
    # Get the highest page id
    # cur.execute(f"SELECT MAX(Page) FROM BAZOSDATA")
    # max_page = cur.fetchone()[0]
    # con.commit()
    # print(f"max page: {max_page}")
    # Select the entire db with the newest batch first
    start_time = time()
    sql = "SELECT * FROM BazosData ORDER BY Batch ASC;"
    temp_sql = "SELECT * FROM BazosData"
    i = 0
    for chunk in pd.read_sql(sql, con, chunksize=10000):
        print(f"Chunk id: {i}")
        i += 1
    print(f"db pull took {time() - start_time} seconds.")


if __name__ == '__main__':
    setup_sql()
    get_page_data()
