import calendar
import datetime as dt

NOW = "DATEDIFF(second, '19700101', sysutcdatetime())"
HOUR = "DATEPART(hour, (DATEADD(second, convert(INT, timestamp), '1970-01-01')))"
DAY = "DATEPART(weekday, (DATEADD(second, convert(INT, timestamp), '1970-01-01')))"


def current_data(row) :
    return (str(round(row[0],2)) + ": AQI=" + str(round(row[2],2)) + ",  CO=" + str(round(row[3],2)) + ", NO=" + str(round(row[4],2))
        + ", NO2=" + str(round(row[5],2)) + ",  O3=" + str(round(row[6],2)) + ", SO2=" + str(round(row[7],2)) + ", NH3=" 
        + str(round(row[8],2)) + ", PM2.5="  + str(round(row[9],2)) + ", PM10=" + str(round(row[10],2)) + "\n"
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
    response = "Average AQI of cities :\n"
    request = "SELECT loc, AVG(aqi) aqi FROM Pollution GROUP BY loc ORDER BY aqi DESC"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        response += "\n" +str(row[0]) + " : " + str(round(row[1],2)) + "\n"
    
        row = cursor.fetchone()
    return response


# Ville la plus polluée
# Ville la moins polluée

 
def hour_ranking(cursor) :
    response = "Average AQI as a function of time is   "  
    request = "SELECT " + HOUR + " hour, AVG(aqi) aqi FROM Pollution GROUP BY " + HOUR + " ORDER BY hour"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        response += "\n" + str(row[0]) + "h : " + str(round(row[1],2))
        row = cursor.fetchone()
    return response

def day_ranking(cursor) :
    response = "Average AQI by day of week:"
    request = "SELECT " + DAY + " day, AVG(aqi) aqi FROM Pollution GROUP BY " + DAY + " ORDER BY day"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        response += "\n" + calendar.day_name[int(row[0]) - 1] + " : " + str(round(row[1],2))
        row = cursor.fetchone()
    return response


def max_pollution_hour(cursor) :
    avg_pollution_hour = "(SELECT " + HOUR + " hour, AVG(aqi) aqi_h FROM Pollution GROUP BY " + HOUR + ") P"
    request = "SELECT hour, aqi_h FROM " + avg_pollution_hour + " INNER JOIN (SELECT MAX(aqi_h) aqi FROM " + avg_pollution_hour + ") M ON (aqi_h = aqi)"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        return "The pollution peak is often reached around " + str(row[0]) + "a.p  with an AQI of :  " + str(round(row[1],2))


def min_pollution_hour(cursor) :
    avg_pollution_hour = "(SELECT " + HOUR + " hour, AVG(aqi) aqi_h FROM Pollution GROUP BY " + HOUR + ") P"
    request = "SELECT hour, aqi_h FROM " + avg_pollution_hour + " INNER JOIN (SELECT MIN(aqi_h) aqi FROM " + avg_pollution_hour + ") M ON (aqi_h = aqi)"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        return "Pollution is lowest around " + str(row[0]) + "h  with an AQI of :" + str(round(row[1],2))


def max_pollution_day(cursor) :
    avg_pollution_day = "(SELECT " + DAY + " day, AVG(aqi) aqi_d FROM Pollution GROUP BY " + DAY + ") P"
    request = "SELECT day, aqi_d FROM " + avg_pollution_day + " INNER JOIN (SELECT MAX(aqi_d) aqi FROM " + avg_pollution_day + ") M ON (aqi_d = aqi)"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        return "The pollution peak is often reached on " + calendar.day_name[int(row[0]) - 1] + " with an AQI of : " + str(round(row[1],2))
        


def min_pollution_day(cursor) :
    avg_pollution_day = "(SELECT " + DAY + " day, AVG(aqi) aqi_d FROM Pollution GROUP BY " + DAY + ") P"
    request = "SELECT day, aqi_d FROM " + avg_pollution_day + " INNER JOIN (SELECT MIN(aqi_d) aqi FROM " + avg_pollution_day + ") M ON (aqi_d = aqi)"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        return "The pollution is lowest on " + calendar.day_name[int(row[0]) - 1] + "with an AQI of : " + str(round(row[1],2))


def wrong_place_wrong_time(cursor) :
    response = "The air quality index is poor at :"
    request = "SELECT DISTINCT loc, " + HOUR + " hour, AVG(aqi) aqi FROM Pollution GROUP BY loc, " + HOUR + " HAVING AVG(aqi) >= 3 ORDER BY loc"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        response += "\n" + str(row[0]) + " à " + str(row[2]) + "h : AQI=" + str(round(row[3],2))
        row = cursor.fetchone()
    return response


def wrong_place_wrong_date(cursor) :
    response = "The air quality index was very poor (AQI=5) at :"
    request = "SELECT  loc, timestamp, aqi FROM Pollution WHERE aqi = 5"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        date = dt.datetime.fromtimestamp(int(row[1]))
        response += "\n" + str(row[0]) + " -> " + str(date)
        row = cursor.fetchone()
    return response

def versailles_current_pollution(cursor) :
    response = "The pollution in Versailles is : "
    request = "SELECT * FROM Pollution WHERE loc = 'Versailles' AND timestamp <= " + NOW + " AND timestamp > " + NOW + " - 3600"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        response += current_data(row)
        row = cursor.fetchone()
    return response

def lille_current_pollution(cursor) :
    response = "The pollution in Lille is :"
    request = "SELECT * FROM Pollution WHERE loc = 'Lille' AND timestamp <= " + NOW + " AND timestamp > " + NOW + " - 3600"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        response += current_data(row)
        row = cursor.fetchone()
    return response


def nice_current_pollution(cursor) :
    response = "The pollution in Nice is :"
    request = "SELECT * FROM Pollution WHERE loc = 'Nice' AND timestamp <= " + NOW + " AND timestamp > " + NOW + " - 3600"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        response += current_data(row)
        row = cursor.fetchone()
    return response


def brest_current_pollution(cursor) :
    response = "The pollution in Brest is : "
    request = "SELECT * FROM Pollution WHERE loc = 'Brest' AND timestamp <= " + NOW + " AND timestamp > " + NOW + " - 3600"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        response += current_data(row)
        row = cursor.fetchone()
    return response


def bayonne_current_pollution(cursor) :
    response = "The pollution in Bayonne is :"
    request = "SELECT * FROM Pollution WHERE loc = 'Bayonne' AND timestamp <= " + NOW + " AND timestamp > " + NOW + " - 3600"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        response += current_data(row)
        current_data(row)
        row = cursor.fetchone()
    return response


def strasbourg_current_pollution(cursor) :
    response = "The pollution in Strasbourg is :"
    request = "SELECT * FROM Pollution WHERE loc = 'Strasbourg' AND timestamp <= " + NOW + " AND timestamp > " + NOW + " - 3600"
    cursor.execute(request)
    row = cursor.fetchone()
    while row:
        response += current_data(row)
        row = cursor.fetchone()
    return response

