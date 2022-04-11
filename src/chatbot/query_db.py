import datetime as dt
import pyodbc

server = "pollution-data.database.windows.net"
database = "AirPollutionDB"
username = "Paco"
password = "uvsq21806570Tostaky78_"
driver = "{ODBC Driver 18 for SQL Server}"

# Quelles sont les données les plus récentes ?
# Quelle est la pollution actuelle en France ?
# Que renseigne les dernières collectes de données ?
# tag : all_news
def recent_news(cursor) :
    now = "DATEDIFF(second, '19700101', sysutcdatetime())"
    cursor.execute(
        "SELECT * FROM Pollution WHERE timestamp <= " + now + " AND timestamp > " + now + " - 3600"
    )
    row = cursor.fetchone()
    while row:
        print(str(row[0]) + " " + str(row[2]) + " " + str(row[3]) + " " + str(row[4]) + " " 
            + str(row[5]) + " " + str(row[6]) + " " + str(row[7]) + " " + str(row[8]) + " " 
            + str(row[9]) + " " + str(row[10])
        )
        row = cursor.fetchone()

if __name__ == "__main__":
    with pyodbc.connect(
        "DRIVER="
        + driver
        + ";SERVER=tcp:"
        + server
        + ";PORT=1433;DATABASE="
        + database
        + ";UID="
        + username
        + ";PWD="
        + password
    ) as conn:
        with conn.cursor() as cursor:
            recent_news(cursor)
    date = dt.datetime.fromtimestamp(int(row[1]))