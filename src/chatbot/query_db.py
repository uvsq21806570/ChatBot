import calendar
import datetime as dt

NOW = "DATEDIFF(second, '19700101', sysutcdatetime())"
HOUR = "DATEPART(hour, (DATEADD(second, convert(INT, timestamp), '1970-01-01')))"
DAY = "DATEPART(weekday, (DATEADD(second, convert(INT, timestamp), '1970-01-01')))"


def current_data(row) :
    return (str(row[0]) + ": AQI=" + str(row[2]) + ",  CO=" + str(row[3]) + ", NO=" + str(row[4])
        + ", NO2=" + str(row[5]) + ",  O3=" + str(row[6]) + ", SO2=" + str(row[7]) + ", NH3=" 
        + str(row[8]) + ", PM2.5="  + str(row[9]) + ", PM10=" + str(row[10]) + "\n"
    )
    

# tag = function name

def recent_news(cursor) :
    response = ""
    request = "SELECT * FROM Pollution WHERE timestamp <= " + NOW + " AND timestamp > " + NOW + " - 3600"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        response += current_data(row)
        row = cursor.fetchone()
    return response


def loc_ranking(cursor) :
    response = "AQI moyen des villes:"
    request = "SELECT loc, AVG(aqi) aqi FROM Pollution GROUP BY loc ORDER BY aqi DESC"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        response += "\n" + str(row[0]) + " : " + str(row[1])
        row = cursor.fetchone()
    return response


# Ville la plus polluée
# Ville la moins polluée

 
def hour_ranking(cursor) :
    response = "AQI moyen en fonction du temps:"
    request = "SELECT " + HOUR + " hour, AVG(aqi) aqi FROM Pollution GROUP BY " + HOUR + " ORDER BY hour"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        response += "\n" + str(row[0]) + "h : " + str(row[1])
        row = cursor.fetchone()
    return response

def day_ranking(cursor) :
    response = "AQI moyen en fonction du jour de la semaine:"
    request = "SELECT " + DAY + " day, AVG(aqi) aqi FROM Pollution GROUP BY " + DAY + " ORDER BY day"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        response += "\n" + calendar.day_name[int(row[0]) - 1] + " : " + str(row[1])
        row = cursor.fetchone()
    return response


def max_pollution_hour(cursor) :
    avg_pollution_hour = "(SELECT " + HOUR + " hour, AVG(aqi) aqi_h FROM Pollution GROUP BY " + HOUR + ") P"
    request = "SELECT hour, aqi_h FROM " + avg_pollution_hour + " INNER JOIN (SELECT MAX(aqi_h) aqi FROM " + avg_pollution_hour + ") M ON (aqi_h = aqi)"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        return "Le pic de pollution est souvent atteint vers " + str(row[0]) + "h avec un AQI de " + str(row[1])


def min_pollution_hour(cursor) :
    avg_pollution_hour = "(SELECT " + HOUR + " hour, AVG(aqi) aqi_h FROM Pollution GROUP BY " + HOUR + ") P"
    request = "SELECT hour, aqi_h FROM " + avg_pollution_hour + " INNER JOIN (SELECT MIN(aqi_h) aqi FROM " + avg_pollution_hour + ") M ON (aqi_h = aqi)"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        return "La pollution est au plus faible vers " + str(row[0]) + "h avec un AQI de " + str(row[1])


def max_pollution_day(cursor) :
    avg_pollution_day = "(SELECT " + DAY + " day, AVG(aqi) aqi_d FROM Pollution GROUP BY " + DAY + ") P"
    request = "SELECT day, aqi_d FROM " + avg_pollution_day + " INNER JOIN (SELECT MAX(aqi_d) aqi FROM " + avg_pollution_day + ") M ON (aqi_d = aqi)"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        return "Le pic de pollution est souvent atteint le " + calendar.day_name[int(row[0]) - 1] + " avec un AQI de " + str(row[1])
        


def min_pollution_day(cursor) :
    avg_pollution_day = "(SELECT " + DAY + " day, AVG(aqi) aqi_d FROM Pollution GROUP BY " + DAY + ") P"
    request = "SELECT day, aqi_d FROM " + avg_pollution_day + " INNER JOIN (SELECT MIN(aqi_d) aqi FROM " + avg_pollution_day + ") M ON (aqi_d = aqi)"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        return "La pollution est au plus faible le " + calendar.day_name[int(row[0]) - 1] + " avec un AQI de " + str(row[1])


def wrong_place_wrong_time(cursor) :
    response = "L'indice de qualité d'air est mauvais à :"
    request = "SELECT DISTINCT loc, " + HOUR + " hour, AVG(aqi) aqi FROM Pollution GROUP BY loc, " + HOUR + " HAVING AVG(aqi) >= 3 ORDER BY loc"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        response += "\n" + str(row[0]) + " à " + str(row[1]) + "h : AQI=" + str(row[2])
        row = cursor.fetchone()
    return response


def wrong_place_wrong_date(cursor) :
    response = "L'indice de qualité d'air était très mauvais (AQI=5) à :"
    request = "SELECT  loc, timestamp, aqi FROM Pollution WHERE aqi = 5"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        date = dt.datetime.fromtimestamp(int(row[1]))
        response += "\n" + str(row[0]) + " -> " + str(date)
        row = cursor.fetchone()
    return response

def versailles_current_pollution(cursor) :
    response = ""
    request = "SELECT * FROM Pollution WHERE loc = 'Versailles' AND timestamp <= " + NOW + " AND timestamp > " + NOW + " - 3600"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        response += current_data(row)
        row = cursor.fetchone()
    return response

def lille_current_pollution(cursor) :
    response = ""
    request = "SELECT * FROM Pollution WHERE loc = 'Lille' AND timestamp <= " + NOW + " AND timestamp > " + NOW + " - 3600"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        response += current_data(row)
        row = cursor.fetchone()
    return response


def nice_current_pollution(cursor) :
    response = ""
    request = "SELECT * FROM Pollution WHERE loc = 'Nice' AND timestamp <= " + NOW + " AND timestamp > " + NOW + " - 3600"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        response += current_data(row)
        row = cursor.fetchone()
    return response


def brest_current_pollution(cursor) :
    response = ""
    request = "SELECT * FROM Pollution WHERE loc = 'Brest' AND timestamp <= " + NOW + " AND timestamp > " + NOW + " - 3600"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        response += current_data(row)
        row = cursor.fetchone()
    return response


def bayonne_current_pollution(cursor) :
    response = ""
    request = "SELECT * FROM Pollution WHERE loc = 'Bayonne' AND timestamp <= " + NOW + " AND timestamp > " + NOW + " - 3600"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        response += current_data(row)
        current_data(row)
        row = cursor.fetchone()
    return response


def strasbourg_current_pollution(cursor) :
    response = ""
    request = "SELECT * FROM Pollution WHERE loc = 'Strasbourg' AND timestamp <= " + NOW + " AND timestamp > " + NOW + " - 3600"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        response += current_data(row)
        row = cursor.fetchone()
    return response

