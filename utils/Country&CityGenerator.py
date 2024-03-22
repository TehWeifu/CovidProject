import pandas as pd

countries = [
    {
        "country_id": 1,
        "name": "Spain"
    },
    {
        "country_id": 2,
        "name": "Italy"
    },
    {
        "country_id": 3,
        "name": "Portugal"
    },
    {
        "country_id": 4,
        "name": "Germany"
    },
    {
        "country_id": 5,
        "name": "France"
    }
]

cities = [
    {
        "city_id": 1,
        "name": "Madrid",
        "country_id": 1,
        "multi_factor": 1.1,
        "latitude": 40.4168,
        "longitude": -3.7038
    },
    {
        "city_id": 2,
        "name": "Barcelona",
        "country_id": 1,
        "multi_factor": 1.2,
        "latitude": 41.3851,
        "longitude": 2.1734
    },
    {
        "city_id": 3,
        "name": "Rome",
        "country_id": 2,
        "multi_factor": 1.3,
        "latitude": 41.9028,
        "longitude": 12.4964
    },
    {
        "city_id": 4,
        "name": "Milan",
        "country_id": 2,
        "multi_factor": 1.4,
        "latitude": 45.4642,
        "longitude": 9.1900
    },
    {
        "city_id": 5,
        "name": "Lisbon",
        "country_id": 3,
        "multi_factor": 1.5,
        "latitude": 38.7223,
        "longitude": -9.1393
    },
    {
        "city_id": 6,
        "name": "Porto",
        "country_id": 3,
        "multi_factor": 1.6,
        "latitude": 41.1579,
        "longitude": -8.6291
    },
    {
        "city_id": 7,
        "name": "Berlin",
        "country_id": 4,
        "multi_factor": 1.7,
        "latitude": 52.5200,
        "longitude": 13.4050
    },
    {
        "city_id": 8,
        "name": "Munich",
        "country_id": 4,
        "multi_factor": 1.8,
        "latitude": 48.1351,
        "longitude": 11.5820
    },
    {
        "city_id": 9,
        "name": "Paris",
        "country_id": 5,
        "multi_factor": 1.9,
        "latitude": 48.8566,
        "longitude": 2.3522
    },
    {
        "city_id": 10,
        "name": "Marseille",
        "country_id": 5,
        "multi_factor": 2.0,
        "latitude": 43.2965,
        "longitude": 5.3698
    }
]

# Save the data into a .parquet file
df_countries = pd.DataFrame(countries)
df_cities = pd.DataFrame(cities)

df_countries.to_parquet("./../data-source/countries.parquet")
df_cities.to_parquet("./../data-source/cities.parquet")
