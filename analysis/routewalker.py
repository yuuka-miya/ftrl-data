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
    return zip(a, b)   

data = {}
df = pd.DataFrame(columns=["daytype", "origin", "dest", "count"])
df_src = pd.read_csv("od201911.csv")

with open('train_routes.json') as json_file:
    data = json.load(json_file)
    
iter = tqdm(data.items())
for code, orig in iter:
  list1 = []
  for destcode, dest in data[code].items():
    for pair in pairwise(dest):
      data1 = df_src.loc[(df_src["ORIGIN_PT_CODE"] == replace_jointcode(code)) & (df_src["DESTINATION_PT_CODE"] == replace_jointcode(destcode))]
      desc = "from " + code + " to " + destcode
      iter.set_description(desc)

      if (data1[data1["DAY_TYPE"] == "WEEKDAY"].empty !=  True):
        list1.append({"daytype": "WEEKDAY", "origin": pair[0], "dest": pair[1], "count": data1[data1["DAY_TYPE"] == "WEEKDAY"]["TOTAL_TRIPS"].values[0]})
      if (data1[data1["DAY_TYPE"] == "WEEKENDS/HOLIDAY"].empty != True):
        list1.append({"daytype": "WEEKENDS/HOLIDAY", "origin": pair[0], "dest": pair[1], "count": data1[data1["DAY_TYPE"] == "WEEKENDS/HOLIDAY"]["TOTAL_TRIPS"].values[0]})

  df = df.append(list1)
df = df.groupby(['daytype', 'origin', 'dest']).sum()
  
df.to_csv("walked_routes_1.csv")