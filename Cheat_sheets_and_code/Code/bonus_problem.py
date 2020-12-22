import pandas as pd

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

foot_fp = r'E://christmas_crisis/foot_traffic_ny.csv'
poi_fp = r'E://christmas_crisis/SGPID_over_80k_POI.csv'


foot_df = pd.read_csv(foot_fp)
poi_df = pd.read_csv(poi_fp)

merge = pd.merge(foot_df, poi_df, how='inner', on='safegraph_place_id')

print(merge.head())

final_df = pd.DataFrame(
    {
        'placekey': merge['placekey'],
        'safegraph_place_id': merge['safegraph_place_id'],
        'location_name': merge['location_name'],
        'street_address': merge['street_address'],
        'city': merge['city'],
        'region': merge['region'],
        'postal_code': merge['postal_code'],
        'raw_visit_counts': merge['raw_visit_counts'],
        'upload_speed': merge['avg_u_kbps']
    }
)

print(final_df)
