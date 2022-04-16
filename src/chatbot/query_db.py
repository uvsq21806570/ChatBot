import datetime as dt
import pyodbc

server = "pollution-data.database.windows.net"
database = "AirPollutionDB"
username = "Paco"
password = "uvsq21806570Tostaky78_"
driver = "{ODBC Driver 18 for SQL Server}"

NOW = "DATEDIFF(second, '19700101', sysutcdatetime())"


def current_data(row) :
    print(str(row[0]) + " " + str(row[2]) + " " + str(row[3]) + " " + str(row[4]) + " " 
        + str(row[5]) + " " + str(row[6]) + " " + str(row[7]) + " " + str(row[8]) + " " 
        + str(row[9]) + " " + str(row[10])
    )


# Quelles sont les données les plus récentes ?
# Quelle est la pollution actuelle en France ?
# Que renseigne les dernières collectes de données ?
# tag : recent_news
def recent_news(cursor) :
    request = "SELECT * FROM Pollution WHERE timestamp <= " + NOW + " AND timestamp > " + NOW + " - 3600"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        current_data(row)
        row = cursor.fetchone()

# A quel moment de la journée atteint t'on le pic de pollution ?
# A quelle heure la pollution est-elle la plus élevée ?
# tag : max_pollution_hour
def max_pollution_hour(cursor) :
    hour = "DATEPART(hour, (DATEADD(second, convert(INT, timestamp), '1970-01-01')))"
    avg_pollution_hour = "(SELECT " + hour + " hour, AVG(aqi) aqi FROM Pollution GROUP BY " + hour + ") P"
    request = "SELECT hour FROM " + avg_pollution_hour + " INNER JOIN (SELECT MAX(aqi) aqi FROM " + avg_pollution_hour + ") M ON (P.aqi = M.aqi)"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        print("Le pic de pollution est souvent atteint vers " + str(row[0]) + " heures.")
        row = cursor.fetchone()

# A quel moment de la journée est t'il le moins risqué de sortir ?
# A quelle heure la pollution est-elle la moins élevée ?
# tag : min_pollution_hour
def min_pollution_hour(cursor) :
    hour = "DATEPART(hour, (DATEADD(second, convert(INT, timestamp), '1970-01-01')))"
    avg_pollution_hour = "(SELECT " + hour + " hour, AVG(aqi) aqi FROM Pollution GROUP BY " + hour + ") P"
    request = "SELECT hour FROM " + avg_pollution_hour + " INNER JOIN (SELECT MIN(aqi) aqi FROM " + avg_pollution_hour + ") M ON (P.aqi = M.aqi)"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        print("La pollution est au plus faible vers " + str(row[0]) + " heures.")
        row = cursor.fetchone()

# Quelles sont les villes les moins polluées ?
# Quelles villes sont les plus polluées ?
# Quel est le classement des villes en fonction de leur taux de pollution ?
def city_ranking(cursor) :
    request = "SELECT loc, AVG(aqi) aqi FROM Pollution GROUP BY loc ORDER BY aqi DESC"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        print(str(row[0]))
        row = cursor.fetchone()

def versailles_current_pollution(cursor) :
    request = "SELECT * FROM Pollution WHERE loc = 'Versailles' AND timestamp <= " + NOW + " AND timestamp > " + NOW + " - 3600"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        current_data(row)
        row = cursor.fetchone()

def lille_current_pollution(cursor) :
    request = "SELECT * FROM Pollution WHERE loc = 'Lille' AND timestamp <= " + NOW + " AND timestamp > " + NOW + " - 3600"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        current_data(row)
        row = cursor.fetchone()

def nice_current_pollution(cursor) :
    request = "SELECT * FROM Pollution WHERE loc = 'Nice' AND timestamp <= " + NOW + " AND timestamp > " + NOW + " - 3600"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        current_data(row)
        row = cursor.fetchone()

def brest_current_pollution(cursor) :
    request = "SELECT * FROM Pollution WHERE loc = 'Brest' AND timestamp <= " + NOW + " AND timestamp > " + NOW + " - 3600"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        current_data(row)
        row = cursor.fetchone()

def bayonne_current_pollution(cursor) :
    request = "SELECT * FROM Pollution WHERE loc = 'Bayonne' AND timestamp <= " + NOW + " AND timestamp > " + NOW + " - 3600"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        current_data(row)
        row = cursor.fetchone()

def strasbourg_current_pollution(cursor) :
    request = "SELECT * FROM Pollution WHERE loc = 'Strasbourg' AND timestamp <= " + NOW + " AND timestamp > " + NOW + " - 3600"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        current_data(row)
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
            max_pollution_hour(cursor)
            min_pollution_hour(cursor)
            versailles_current_pollution(cursor)   
    
# date = dt.datetime.fromtimestamp(int(row[1]))