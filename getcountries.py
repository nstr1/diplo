from geopy.geocoders import Nominatim
import numpy as np
import json
import pandas as pd

import ast

geolocator = Nominatim(user_agent="http")
def geolocate(country):
    try:
        loc = geolocator.geocode(country, language="english", addressdetails=True)
        return loc.raw['address']['country']
    except:
        return np.nan

print("reading csv ...")
df = pd.read_csv('reduced.csv')
print("finished reading csv")

df['country'] = np.nan

file = open("cache.txt", "r")

data = file.read()
js = json.loads(data)

file.close()

cache = js

print("getting locs...")

for i in df.index:
    
    location = df.at[i, 'location']
    if i == 100000:
        print(df.columns)
        df = df.dropna()

        print("writing...")
        df.to_csv("countries.csv")


    if i % 100 == 0:
        with open('cache.txt', 'w') as f:
            f.write(json.dumps(cache))
        print("export cache")

    if location in cache.keys():
        df.at[i, 'country'] = cache[location]
        print(f"{i}: used {location} from cache")
        continue
    else:
        country = geolocate(location)
        print(f"{i}: added {location} to cache")
        cache[location] = country
        df.at[i, 'country'] = country
        
print(df.columns)
df = df.dropna()

print("writing...")
df.to_csv("countries.csv")
