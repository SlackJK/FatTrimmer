import pandas as pd
import pyodbc
from time import time


# SQL setup
SQL_SERVER_IP = ""
SQL_SERVER_PORT = ""
SQL_SERVER_DATABASE = ""
SQL_SERVER_USERID = ""
SQL_SERVER_PASSWORD = ""
SQL_CHUNK_SIZE = 10000

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
    temp_sql = "SELECT TOP(1000) * FROM BazosData"
    i = 0
    df = pd.DataFrame()
    for chunk in pd.read_sql(sql, con, chunksize=SQL_CHUNK_SIZE):
        print(f"Chunk id: {i}")
        df = pd.concat([df, chunk])
        i += 1
    df = df[df.Batch != -1]  # Fix one off row in sql db
    df.reset_index(inplace=True)  # Fix index after filter
    print(f"db pull took {time() - start_time} seconds.")
    # Describe df
    print(df.head())
    print(df.describe())
    print(df.dtypes)
    print(df.shape)
    return df


def process_data(df):
    """
    Write code here
    """
    pass

if __name__ == '__main__':
    setup_sql()
    df = get_page_data()
    process_data(df)
