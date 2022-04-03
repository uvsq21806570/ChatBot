import requests
from datetime import timedelta, datetime as dt
from geopy.geocoders import Nominatim


GEOLOCATOR = Nominatim(user_agent="Your Name")
NOW = dt.now()


def coordinates_from_location(location):
    coordinates = GEOLOCATOR.geocode(location)
    return coordinates.latitude, coordinates.longitude


def location_from_coordinates(coordinates):
    str_coordinates = str(coordinates["lat"]) + ", " + str(coordinates["lon"])
    location = GEOLOCATOR.reverse(str_coordinates)
    return (location.raw["address"])["municipality"]


def collect_data_from_city(location="Paris", start=NOW - timedelta(7), end=NOW):
    start = round(dt.timestamp(start))
    end = round(dt.timestamp(end))
    location = coordinates_from_location(location)
    address = (
        "http://api.openweathermap.org/data/2.5/air_pollution/history?lat="
        + str(location[0])
        + "&lon="
        + str(location[1])
        + "&start="
        + str(start)
        + "&end="
        + str(end)
        + "&appid=a11c2b0cb799b0a23392091a02453e2f"
    )
    response = requests.get(address)
    return response.json()


def collect_data():
    return [
        collect_data_from_city("Versailles"),
        collect_data_from_city("Marseille"),
        collect_data_from_city("Brest"),
    ]


if __name__ == "__main__":
    collected_data = collect_data()
