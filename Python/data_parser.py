import pandas as pd
import pyodbc
from time import time
from json import load


# SQL setup
with open(r"/home/andy/PycharmProjects/FatTrimmer/Python/sql-config.json") as f:
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
    temp_sql = "SELECT TOP(100000) * FROM BazosData ORDER BY Batch ASC;"
    i = 0
    df = pd.DataFrame()
    for chunk in pd.read_sql(temp_sql, con, chunksize=SQL_CHUNK_SIZE):
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
    print("Processing data...")
    start_time = time()
    # use df.pipe and df.eq?
    output = pd.DataFrame(columns=["Page", "DeltaTime"])
    duplicates = set()
    for index, listing in df.iterrows():
        # if the title is unique
        if listing["Title"] not in duplicates:
            # TODO takes too long, add progress counter
            batch, page = listing["Batch"], listing["Page"]
            bazos_page = df[df["Page"] == page]
            output = output.append(pd.Series([page, 1000]), ignore_index=True)
            duplicates.add(listing["Title"])
    print(f"anal took {round((time() - start_time), 2)} seconds.")
    # print(f"the duplicates set has {len(duplicates)} elements")
    # print(f"it takes {sys.getsizeof(duplicates)} bytes of memory")
    return output


# To find where scrapes end, iterate over and keep track of largest page, when all pages up to that are covered: consider that the end.

if __name__ == '__main__':
    pd.set_option("display.max_columns", None)
    # setup_sql()
    # df = get_page_data()
    # df.to_csv(r"BazosData.csv")  # FIXME index row getting saved
    print("reading csv..")
    start_time = time()
    df = pd.DataFrame()
    columns = ["Title", "Price", "PostTime", "Description", "ItemLink", "TimeOfRun", "Batch", "Page", "UniqueListingID"]
    for chunk in pd.read_csv(r"/home/andy/PycharmProjects/FatTrimmer/BazosData100000.csv", usecols=columns, chunksize=10**6):
        df = pd.concat([df, chunk])
    # df[["PostTime", "TimeOfRun"]] = df[["PostTime", "TimeOfRun"]].apply(pd.to_datetime)
    # df["PostTime"] = pd.to_datetime(df["PostTime"], format=r"[%d.%m. %Y]")  # TODO hash to reduce complexity
    print(f"reading csv took {round((time()-start_time), 2)} seconds")
    print(df.head())
    print(process_data(df))
