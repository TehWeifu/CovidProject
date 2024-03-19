import pandas as pd

countries = [
    {
        "id": 1,
        "name": "Spain"
    },
    {
        "id": 2,
        "name": "Italy"
    },
    {
        "id": 3,
        "name": "Portugal"
    },
    {
        "id": 4,
        "name": "Germany"
    },
    {
        "id": 5,
        "name": "France"
    }
]

cities = [
    {
        "city_id": 1,
        "name": "Madrid",
        "country_id": 1,
        "multi_factor": 1.1
    },
    {
        "city_id": 2,
        "name": "Barcelona",
        "country_id": 1,
        "multi_factor": 1.2
    },
    {
        "city_id": 3,
        "name": "Rome",
        "country_id": 2,
        "multi_factor": 1.3
    },
    {
        "city_id": 4,
        "name": "Milan",
        "country_id": 2,
        "multi_factor": 1.4
    },
    {
        "city_id": 5,
        "name": "Lisbon",
        "country_id": 3,
        "multi_factor": 1.5
    },
    {
        "city_id": 6,
        "name": "Porto",
        "country_id": 3,
        "multi_factor": 1.6
    },
    {
        "city_id": 7,
        "name": "Berlin",
        "country_id": 4,
        "multi_factor": 1.7
    },
    {
        "city_id": 8,
        "name": "Munich",
        "country_id": 4,
        "multi_factor": 1.8
    },
    {
        "city_id": 9,
        "name": "Paris",
        "country_id": 5,
        "multi_factor": 1.9
    },
    {
        "city_id": 10,
        "name": "Marseille",
        "country_id": 5,
        "multi_factor": 2.0
    }
]

# Save the data into a .parquet file
df_countries = pd.DataFrame(countries)
df_cities = pd.DataFrame(cities)

df_countries.to_parquet("data-source/countries.parquet")
df_cities.to_parquet("data-source/cities.parquet")
