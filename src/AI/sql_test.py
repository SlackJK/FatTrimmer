import pyodbc

# SQL setup
SQL_SERVER_IP = ""
SQL_SERVER_PORT = "1433"
SQL_SERVER_DATABASE = "bazos"
SQL_SERVER_USERID = ""
SQL_SERVER_PASSWORD = ""

con = pyodbc.connect(
        "Driver={SQL Server Native Client 11.0};"
        f"Server={SQL_SERVER_IP},{SQL_SERVER_PORT};"
        f"Database={SQL_SERVER_DATABASE};"
        f"UID={SQL_SERVER_USERID};"
        f"PWD={SQL_SERVER_PASSWORD}"
)
# cur = con.cursor()
#
#
# cur.execute("SELECT * FROM FatTrimmerData")
#
# data = cur.fetch_one()
#
# con.commit()

print(data)
