import pandas as pd


### setting display options like this makes reading multi column DFs much easier
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

poi_fp = r'E:/christmas_crisis/SG_POI_data.csv' # File with Safegraph POI data
geo_merged_fp = r'E://christmas_crisis/SGPID_over_80k_POI.csv' # File with speeds and sgpid

poi_df = pd.read_csv(poi_fp)
geo_merged_df = pd.read_csv(geo_merged_fp)

merge = pd.merge(poi_df, geo_merged_df, how='inner', on='safegraph_place_id')

print(merge)

ultimate_addr = merge[merge.avg_u_kbps == merge.avg_u_kbps.max()]

print(ultimate_addr)


