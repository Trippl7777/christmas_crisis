import pandas as pd


### setting display options like this makes reading multi column DFs much easier
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

poi_fp = r'E:/christmas_crisis/SG_POI_data.csv' # File with Safegraph POI data

slist_fp = r'E://christmas_crisis/Santas Starbucks List.csv' # File with Starbucks addresses

poi_df = pd.read_csv(poi_fp)
df = pd.read_csv(slist_fp)

print(df.head())
print(poi_df.head())

merged = pd.merge(df, poi_df, how='inner', on='placekey') #Merge the data on our Placekeys to get the lat lng values

print(merged)

# Output file to your project folder

merged.to_csv(r'E://christmas_crisis/slist_w_pkey_and_poi_info.csv', encoding='utf-8', index=False)

