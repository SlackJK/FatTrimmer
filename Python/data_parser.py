import pandas as pd
import pyodbc
from time import time
from json import load


# SQL setup
with open(r"Python\sql_config.json") as f:
    SQL_CONFIG = load(f)
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
        f"Server={SQL_CONFIG['ip']},{SQL_CONFIG['port']};"
        f"Database={SQL_CONFIG['database']};"
        f"UID={SQL_CONFIG['uid']};"
        f"PWD={SQL_CONFIG['password']}"
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
    start_time = time()
    # df["IsDuplicate"] = 
    # use df.pipe and df.eq?
    print(f"anal took {time() - start_time} seconds.")



# To find where scrapes end, iterate over and keep track of largest page, when all pages up to that are covered: consider that the end.

if __name__ == '__main__':
    setup_sql()
    # df = get_page_data()
    # df.to_csv(r"BazosData.csv")
    print("reading csv..")
    df = pd.read_csv(r"BazosData.csv")
    print("done")
    process_data(df)
