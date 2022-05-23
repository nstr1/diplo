import pandas as pd 
import os
import csv
import gc
from pathlib import Path 

csv_collection = []
for dirname, _, filenames in os.walk('data'):
    filenames.sort()
    for filename in filenames:
        fullpath = os.path.join(dirname, filename)
        csv_collection.append(fullpath)
        print("Added {fullpath}".format(fullpath=fullpath))

columns = ['username','location', 'followers', 'tweetcreatedts', 'retweetcount', 'text', 'hashtags', 'language']

csv_collection.sort()
dataframe_collection = []
for csvfile in csv_collection:
    df = pd.read_csv(csvfile, engine='python', compression='gzip',encoding='utf-8', quoting=csv.QUOTE_ALL)
    df = df[columns]
    df = df.dropna()
    dataframe_collection.append(df)
    print("{}| {} rows".format(Path(csvfile).name, len(df)))
    
gc.collect()

df_combined = pd.concat(dataframe_collection, axis=0)
df_combined['tweetcreatedts'] = pd.to_datetime(df_combined['tweetcreatedts'], errors='coerce')
df_combined.reset_index(inplace=True, drop=True)

df_combined.to_csv('merged.csv')

print(df_combined.shape)