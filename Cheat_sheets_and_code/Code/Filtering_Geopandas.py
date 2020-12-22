import pandas as pd
import pyarrow
from shapely.geometry import Point, Polygon
from shapely import wkt
import geopandas as gpd
from geopandas.tools import sjoin

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


fp = r'E://christmas_crisis/2020-01-01_performance_fixed_tiles.parquet'
santa_list = r'E://christmas_crisis/slist_w_pkey_and_poi_info.csv'

df2 = pd.read_csv(santa_list)

print(df2.head())

#Turning lat, lng into geo points. remember geopandas reads (lng, lat) NOT (lat, lng)

POI = []

for index, row in df2.iterrows():
    p = Point(row['longitude'], row['latitude'])
    POI.append(p)

print(POI)

df2['points'] = POI

geo_sg = pd.DataFrame(
    {
        'safegraph_place_id': df2['safegraph_place_id'],
        'geometry': df2['points']
    }
)

my_geo_sg = gpd.GeoDataFrame(geo_sg, geometry='geometry') # Setting the geometry section of DF for GPD to know what to join on

# Using the Pyarrow engine to read the parquet
df = pd.read_parquet(fp, engine='pyarrow')

df['geometry'] = df['tile'].apply(wkt.loads)
my_geo_df = gpd.GeoDataFrame(df, geometry='geometry') # Setting the geometry section of DF for GPD to know what to join on

print(df.head())
pointInPolys = sjoin(my_geo_sg, my_geo_df, how='inner') #Sjoin to use the aforementioned geometry columns to find points within polygons
print(pointInPolys.head())
print(pointInPolys.shape)

possible_choice_df = pointInPolys[pointInPolys['avg_u_kbps'] > 80000] # Only accepting the POI with over 80mbps (>80000kbps)

print(possible_choice_df)
print(possible_choice_df.shape)

panda_df = pd.DataFrame(possible_choice_df) # Convert back to Pandas for export

panda_df.to_csv(r'E://christmas_crisis/SGPID_over_80k_POI.csv', encoding='utf-8', index=False)
