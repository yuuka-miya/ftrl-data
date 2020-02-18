import json
import itertools
import pandas as pd
import numpy as np
from tqdm import tqdm

joint_codes = {
"NS1": "EW24-NS1",
"NS24": "CC1-NE6-NS24",
"NS25" : "EW13-NS25",
"NS26": "EW14-NS26",
"CC4": "CC4-DT15",
"NE16": "NE16-STC",
"NE17": "NE17-PTC"
}

def replace_jointcode(code):
  if code in joint_codes.keys():
    return joint_codes[code]
  return code


def pairwise(iterable):
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b = itertools.tee(iterable)
    next(b, None)
    return list(a), list(b)  

data = {}
df = pd.DataFrame()
df_src = pd.read_csv("od201911.csv")

with open('train_routes.json') as json_file:
    data = json.load(json_file)
    
iter = tqdm(data.items())
for code, orig in iter:
#code = "BP10"

  for destcode, dest in data[code].items():
    a, b = pairwise(dest)
    a.pop()
    pairs_len = len(b)
    df_temp = pd.DataFrame()
    data1 = pd.DataFrame(df_src.loc[(df_src["ORIGIN_PT_CODE"] == replace_jointcode(code)) & (df_src["DESTINATION_PT_CODE"] == replace_jointcode(destcode))])
    for data_row in data1.itertuples(index=False):
      #iter.set_description(code, destcode)
      df_data = pd.DataFrame([data_row])
      df_data = df_data.loc[df_data.index.repeat(pairs_len)]
      df_data["ORIGIN_PT_CODE"] = a
      df_data["DESTINATION_PT_CODE"] = b
      df_temp = df_temp.append(df_data, ignore_index=True)
    
    #print(df_temp)
    
    if len(df_temp.index) > 0:
      df_temp = df_temp.groupby(['DAY_TYPE', 'ORIGIN_PT_CODE', 'DESTINATION_PT_CODE']).sum()
      df = df.append(df_temp)
  
  df = df.groupby(['DAY_TYPE', 'ORIGIN_PT_CODE', 'DESTINATION_PT_CODE']).sum()
    
df["TOTAL_TRIPS"] = df["TOTAL_TRIPS"].round(1)
df.to_csv("walked_routes_1.csv")