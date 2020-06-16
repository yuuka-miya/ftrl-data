import pandas as pd
import numpy as np
import os
import datetime
import json
import itertools

from tqdm import tqdm
#from multiprocessing import Pool

counter = 0

joint_codes = {
"EW24": "NS1",
"CC1": "NS24",
"NE6": "NS24",
"EW13" : "NS25",
"EW14": "NS26",
"DT15": "CC4",
"STC": "NE16",
"PTC": "NE17"
}

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

def replace_jointcode(code):
  code = code.split('/')[0]
  if code in joint_codes.keys():
    return joint_codes[code]
  if code in interchange_codes.keys():
    return joint_codes[code]
  return code
  
#http://www.racketracer.com/2016/07/06/pandas-in-parallel/

# def parallelize_dataframe(df, func):
    # df_split = np.array_split(df, num_partitions)
    # pool = Pool(num_cores)
    # df = pd.concat(pool.map(func, df_split))
    # pool.close()
    # pool.join()
    # return df

def pairwise(iterable):
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b = itertools.tee(iterable)
    next(b, None)
    return list(a), list(b)
    
def unpack_column(data_row):
  global df_fin
  global df_temp
  global counter
  
  orig = replace_jointcode(data_row["ORIGIN_PT_CODE"])
  dest = replace_jointcode(data_row["DESTINATION_PT_CODE"])
  a, b = pairwise(data[orig][dest])
  a.pop()
  pairs_len = len(b)
  df_data = pd.DataFrame([data_row])
  df_data = df_data.loc[df_data.index.repeat(pairs_len)]
  df_data["ORIGIN_PT_CODE"] = a
  df_data["DESTINATION_PT_CODE"] = b
  if len(df_data.index) > 0:
     df_temp = pd.concat([df_temp, df_data])
     #make sure df_temp doesn't get too big
     counter = counter + 1
     if counter > 1000:
        df_temp = df_temp.groupby(['DAY_TYPE', 'ORIGIN_PT_CODE', 'DESTINATION_PT_CODE', 'TIME_PER_HOUR']).sum()
        df_fin = df_fin.append(df_temp)
        counter = 0

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
df = df[df['ORIGIN_PT_CODE'] != df['DESTINATION_PT_CODE']]
df1 = df.drop(columns=['multiplier'])

#congestion analysis
with open('train_routes_nx.json') as json_file:
    data = json.load(json_file)
    
tqdm.pandas()

df_fin = pd.DataFrame()
df_temp = pd.DataFrame()
df1.progress_apply(unpack_column, axis=1)

#pack up the stragglers
df_temp = df_temp.groupby(['DAY_TYPE', 'ORIGIN_PT_CODE', 'DESTINATION_PT_CODE', 'TIME_PER_HOUR']).sum()
df_fin = df_fin.append(df_temp)

df_fin = df_fin.groupby(['DAY_TYPE', 'ORIGIN_PT_CODE', 'DESTINATION_PT_CODE', 'TIME_PER_HOUR']).sum()

df_fin1 = pd.pivot_table(df_fin, index=['DAY_TYPE', 'ORIGIN_PT_CODE', 'DESTINATION_PT_CODE'], columns=["TIME_PER_HOUR"], aggfunc={'TOTAL_TRIPS': np.sum})

df_fin = df_fin.groupby(['DAY_TYPE', 'ORIGIN_PT_CODE', 'DESTINATION_PT_CODE']).sum()

df_fin.to_csv(os.path.join(os.getcwd(), "processed_data", month, "cda_nx_opt_train_" + month + "_summary_" + datetime.datetime.now().strftime('%Y%m%d%H%M%S') + ".csv"))
df_fin1.to_csv(os.path.join(os.getcwd(), "processed_data", month, "cda_nx_opt_train_" + month + "_byhour_" + datetime.datetime.now().strftime('%Y%m%d%H%M%S') + ".csv"))