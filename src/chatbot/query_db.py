from calendar import weekday
import calendar
import datetime as dt
import pyodbc

server = "pollution-data.database.windows.net"
database = "AirPollutionDB"
username = "Paco"
password = "uvsq21806570Tostaky78_"
driver = "{ODBC Driver 18 for SQL Server}"

NOW = "DATEDIFF(second, '19700101', sysutcdatetime())"
HOUR = "DATEPART(hour, (DATEADD(second, convert(INT, timestamp), '1970-01-01')))"
DAY = "DATEPART(weekday, (DATEADD(second, convert(INT, timestamp), '1970-01-01')))"


def current_data(row) :
    return (str(row[0]) + " " + str(row[2]) + " " + str(row[3]) + " " + str(row[4]) + " " 
        + str(row[5]) + " " + str(row[6]) + " " + str(row[7]) + " " + str(row[8]) + " " 
        + str(row[9]) + " " + str(row[10])
    )
    

# tag = function name

# What is the pollution of the last few minutes in France ?
# What is the current air quality in each city ?
# Give me the live pollution rate in the whole France.
def recent_news(cursor) :
    request = "SELECT * FROM Pollution WHERE timestamp <= " + NOW + " AND timestamp > " + NOW + " - 3600"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        current_data(row)
        row = cursor.fetchone()


# Which are the least polluted cities ?
# Which cities are the most polluted ?
# Which are the most polluted locations ?
# Which locations are the least polluted ?
# How do cities rank according to their pollution levels ? 
def loc_ranking(cursor) :
    request = "SELECT loc, AVG(aqi) aqi FROM Pollution GROUP BY loc ORDER BY aqi DESC"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        print(str(row[0]) + " : " + str(row[1]))
        row = cursor.fetchone()


# Ville la plus polluée
# Ville la moins polluée


# What is the air quality index according to the time of day ?
# Does air quality change during the day ?
# Is air quality worse in morning or evening ?
# Is air quality worse during the day ?
def hour_ranking(cursor) :
    request = "SELECT " + HOUR + " hour, AVG(aqi) aqi FROM Pollution GROUP BY " + HOUR + " ORDER BY hour"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        print(str(row[0]) + " heures : " + str(row[1]))
        row = cursor.fetchone()


# What is the air quality index according to the day of the week ?
# Does air quality change over the week ?
# Is air quality worse during the week-end or not ?
def day_ranking(cursor) :
    request = "SELECT " + DAY + " day, AVG(aqi) aqi FROM Pollution GROUP BY " + DAY + " ORDER BY day"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        print(str(row[0]) + " " + calendar.day_name[int(row[0]) - 1] + " : " + str(row[1]))
        row = cursor.fetchone()

# At what time of day do we reach the pollution peak?
# At what time is the pollution highest?
# At what time is the air very bad ?
# At what time of day can it be dangerous to go outside due to poor air quality?
# tag : max_pollution_hour
def max_pollution_hour(cursor) :
    avg_pollution_hour = "(SELECT " + HOUR + " hour, AVG(aqi) aqi_h FROM Pollution GROUP BY " + HOUR + ") P"
    request = "SELECT hour, aqi_h FROM " + avg_pollution_hour + " INNER JOIN (SELECT MAX(aqi_h) aqi FROM " + avg_pollution_hour + ") M ON (aqi_h = aqi)"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        print("Le pic de pollution est souvent atteint vers " + str(row[0]) + " heures avec un AQI de " + str(row[1]))
        row = cursor.fetchone()


# What time of day is air quality the best ?
# At what time of day is it least risky to go out ? 
# At what time is the air good ?
# What time is the lowest pollution ?
# tag : min_pollution_hour
def min_pollution_hour(cursor) :
    avg_pollution_hour = "(SELECT " + HOUR + " hour, AVG(aqi) aqi_h FROM Pollution GROUP BY " + HOUR + ") P"
    request = "SELECT hour, aqi_h FROM " + avg_pollution_hour + " INNER JOIN (SELECT MIN(aqi_h) aqi FROM " + avg_pollution_hour + ") M ON (aqi_h = aqi)"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        print("La pollution est au plus faible vers " + str(row[0]) + " heures avec un AQI de " + str(row[1]))
        row = cursor.fetchone()


# What is the most polluted day ?
# Which day is more polluted than the others?
# On what day is the air quality worst?
def max_pollution_day(cursor) :
    avg_pollution_day = "(SELECT " + DAY + " day, AVG(aqi) aqi_d FROM Pollution GROUP BY " + DAY + ") P"
    request = "SELECT day, aqi_d FROM " + avg_pollution_day + " INNER JOIN (SELECT MAX(aqi_d) aqi FROM " + avg_pollution_day + ") M ON (aqi_d = aqi)"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        print("Le pic de pollution est souvent atteint le " + calendar.day_name[int(row[0]) - 1] + " avec un AQI de " + str(row[1]))
        row = cursor.fetchone()


# What is the least polluted day ?
# Which day is less polluted than the others?
# On what day is the air quality good?
def min_pollution_day(cursor) :
    avg_pollution_day = "(SELECT " + DAY + " day, AVG(aqi) aqi_d FROM Pollution GROUP BY " + DAY + ") P"
    request = "SELECT day, aqi_d FROM " + avg_pollution_day + " INNER JOIN (SELECT MIN(aqi_d) aqi FROM " + avg_pollution_day + ") M ON (aqi_d = aqi)"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        print("La pollution est au plus faible le " + calendar.day_name[int(row[0]) - 1] + " avec un AQI de " + str(row[1]))
        row = cursor.fetchone()


# Where and when is it highly inadvisable to go out ?
# Where and when can it be very dangerous to leave your home
# What are the coordinates and time frame of the poor air quality index ?
def wrong_place_wrong_time(cursor) :
    request = "SELECT DISTINCT loc, " + HOUR + " hour, AVG(aqi) aqi FROM Pollution GROUP BY loc, " + HOUR + " HAVING AVG(aqi) >= 3"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        print(str(row[0]) + " à " + str(row[1]) + " heures : " + str(row[2]))
        row = cursor.fetchone()


# When and where was the pollution level very bad and potentially deadly ?
# Where and when the air quality index was extremely high ?
# Where and when was it very dangerous to leave your home ?
def wrong_place_wrong_date(cursor) :
    request = "SELECT  loc, timestamp, aqi FROM Pollution WHERE aqi = 5"
    cursor.execute(request)
    row = cursor.fetchone()
    print("AQI = 5 :")
    while row:
        date = dt.datetime.fromtimestamp(int(row[1]))
        print(str(row[0]) + " -> " + str(date))
        row = cursor.fetchone()


# What is the current pollution in Versailles
# What is the air quality in Versailles in the last few minutes?
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
            wrong_place_wrong_date(cursor)