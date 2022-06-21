import pandas as pd
import numpy as np
import os
import datetime
import json
import itertools

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

interchange_codes = {
    "BP1": "NS4",
    "CC15": "NS17",
    "CE2": "NS27",
    "CC9": "EW8",
    "DT14": "EW12",
    "NE3": "EW16",
    "CC22": "EW21",
    "DT35": "CG1",
    "CC29": "NE1",
    "DT19": "NE4",
    "DT12": "NE7",
    "CC13": "NE12",
    "STC": "NE16",
    "PTC": "NE17",
    "DT26": "CC10",
    "DT9": "CC19",
    "DT16": "CE1",
    "TE2": "NS9",
    "TE9": "CC17",
    "TE11": "DT10",
    "TE14": "NS22",
    "TE17": "EW16",
    "TE20": "NS27",
    "TE31": "DT37",
    "FL1": "CC32",
    "JS1": "NS4",
    "JS8": "EW27",
    "JE5": "EW24-NS1"
    
    }

month = input("Dataset for (YYYYMM): ")
weekdays = input("Number of weekdays: ")
weekdays = int(weekdays)

specials = input("Number of weekends and holidays: ")
specials = int(specials)

if not os.path.exists(os.path.join(os.getcwd(), "processed_data", month)):
    os.mkdir(os.path.join(os.getcwd(), "processed_data", month))

total = specials + weekdays

#now for orig dest

in_file = os.path.join(os.getcwd(), "..", "raw_data", month, "origin_destination_train_" + month + ".csv")

df = pd.read_csv(in_file)

#orig dest
df['multiplier'] = df['DAY_TYPE']

df = df.replace({'ORIGIN_PT_CODE': interchange_codes, 'DESTINATION_PT_CODE': interchange_codes, 'multiplier': {'WEEKENDS/HOLIDAY': specials, 'WEEKDAY': weekdays}})
df['TOTAL_TRIPS'] = (df['TOTAL_TRIPS'] / df['multiplier']).round(0)
df = df[df['TOTAL_TRIPS'] !=0]
df1 = df.drop(columns=['multiplier'])

#congestion analysis
df_fin = pd.DataFrame()
with open('train_routes_nx.json') as json_file:
    data = json.load(json_file)
    
iter = tqdm(data.items())
for code, orig in iter:
#code = "BP10"
  for destcode, dest in data[code].items():
    a, b = pairwise(dest)
    a.pop()
    pairs_len = len(b)
    df_temp = pd.DataFrame()
    data1 = pd.DataFrame(df1.loc[(df["ORIGIN_PT_CODE"] == replace_jointcode(code)) & (df1["DESTINATION_PT_CODE"] == replace_jointcode(destcode))])
    for data_row in data1.itertuples(index=False):
      df_data = pd.DataFrame([data_row])
      df_data = df_data.loc[df_data.index.repeat(pairs_len)]
      df_data["ORIGIN_PT_CODE"] = a
      df_data["DESTINATION_PT_CODE"] = b
      df_temp = df_temp.append(df_data, ignore_index=True)
    
    if len(df_temp.index) > 0:
      df_temp = df_temp.groupby(['DAY_TYPE', 'ORIGIN_PT_CODE', 'DESTINATION_PT_CODE', 'TIME_PER_HOUR']).sum()
      df_fin = df_fin.append(df_temp)
    
    df_fin = df_fin.groupby(['DAY_TYPE', 'ORIGIN_PT_CODE', 'DESTINATION_PT_CODE', 'TIME_PER_HOUR']).sum()
    

df_fin1 = pd.pivot_table(df_fin, index=['DAY_TYPE', 'ORIGIN_PT_CODE', 'DESTINATION_PT_CODE'], columns=["TIME_PER_HOUR"], aggfunc={'TOTAL_TRIPS': np.sum})

df_fin = df_fin.groupby(['DAY_TYPE', 'ORIGIN_PT_CODE', 'DESTINATION_PT_CODE']).sum()

df_fin.to_csv(os.path.join(os.getcwd(), "processed_data", month, "cda_nx_train_" + month + "_summary_" + datetime.datetime.now().strftime('%Y%m%d%H%M%S') + ".csv"))
df_fin1.to_csv(os.path.join(os.getcwd(), "processed_data", month, "cda_nx_train_" + month + "_byhour_" + datetime.datetime.now().strftime('%Y%m%d%H%M%S') + ".csv"))