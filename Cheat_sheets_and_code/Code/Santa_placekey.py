from safegraph_py_functions import safegraph_py_functions as sgpy
import pandas as pd
import numpy as np
import json
from placekey.api import PlacekeyAPI
import os
import requests

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

placekey_api_key = "XXXXXXXXXXXXXXXXXXXXXXXXX" # Add your placekey API key here


pk_api = PlacekeyAPI(placekey_api_key)

data_path = r'E://christmas_crisis/Santas Starbucks List.csv' #### add your FP here



my_dtypes = {'name' : str,
             'address':str,
             'city': str,
             'region': str,
             'postal_code': str}

orig_df = pd.read_csv(data_path, dtype=my_dtypes)
print(orig_df.shape)
orig_df.head()

li = []

for x in range(orig_df.shape[0]):
    li.append(f"{x}")

orig_df['id_num'] = li

query_id_col = 'id_num' # this column in your data should be unique for every row
column_map = {query_id_col: "query_id",
              'name' : "location_name",
              'address' : "street_address",
              'city': "city",
              'region': "region",
              'postal_code': "postal_code",
             }

df_for_api = orig_df.rename(columns=column_map)
cols = list(column_map.values())
df_for_api = df_for_api[cols]

# add missing hard-coded columns
df_for_api['iso_country_code'] = 'US'
df_for_api.head()

df_clean = df_for_api.copy()
possible_bad_values = ["", " ", "null", "Null", "None", "nan", "Nan"] # Any other dirty data you need to clean up?
for bad_value in possible_bad_values:
  df_clean = df_clean.replace(to_replace=bad_value, value=np.nan)

print("FYI data missing from at least 1 column in the following number of rows:")
print(df_clean.shape[0] - df_clean.dropna().shape[0])
print("Some examples of rows with missing data")
df_clean[df_clean.isnull().any(axis=1)].head()


data_jsoned = json.loads(df_clean.to_json(orient="records"))
print("number of records: ", len(data_jsoned))
print("example record:")
print(data_jsoned[0])

single_place_example = data_jsoned[0]
print("input: \n",single_place_example)
print("\nresult: \n",pk_api.lookup_placekey(**single_place_example))

responses = pk_api.lookup_placekeys(data_jsoned,
                                    strict_address_match=False,
                                    strict_name_match=False,
                                    verbose=True)

def clean_api_responses(data_jsoned, responses):

    print("number of original records: ", len(data_jsoned))
    print("total individual queries returned:", len(responses))

    # filter out invalid responses
    responses_cleaned = [resp for resp in responses if 'query_id' in resp]
    print("total successful query responses:", len(responses_cleaned))
    return(responses_cleaned)

responses_cleaned = clean_api_responses(data_jsoned, responses)

df_placekeys = pd.read_json(json.dumps(responses_cleaned), dtype={'query_id':str})
df_placekeys.head(10)

df_join_placekey = pd.merge(orig_df, df_placekeys, left_on=query_id_col, right_on="query_id", how='left')
final_cols = list(df_placekeys.columns) + list(orig_df.columns)
df_join_placekey = df_join_placekey[final_cols]
df_join_placekey.head()

print("Summary of results:")
total_recs = df_join_placekey.shape[0]
print("total records:", total_recs)
print("records with a placekey: {0} | {1:.2f}%".format(df_join_placekey[~df_join_placekey.placekey.isnull()].shape[0],  df_join_placekey[~df_join_placekey.placekey.isnull()].shape[0]*100/total_recs))
print("records missing a placekey: {0} | {1:.2f}% [Missing placekeys may be due to exceeding API rate limit]".format(df_join_placekey[df_join_placekey.placekey.isnull()].shape[0], df_join_placekey[df_join_placekey.placekey.isnull()].shape[0]*100/total_recs))
print("records missing a query_id: {0} | {1:.2f}% [Invalid query inputs or missing result due to exceeded API rate limit]".format(df_join_placekey[df_join_placekey.query_id.isnull()].shape[0], df_join_placekey[df_join_placekey.query_id.isnull()].shape[0]*100/total_recs))



output_cols = ['placekey'] + list(orig_df.columns)
output_df = df_join_placekey[output_cols]



output_df.to_csv(f"E://christmas_crisis/placekey_output_file.csv", index=False)

output_df.head(5)




