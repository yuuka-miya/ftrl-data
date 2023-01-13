# This file was used to produce December 2022 data. 

import pandas as pd
import numpy as np
import os
import datetime

# same paid area. impossible to enforce which "line" pax exited out of
# so they just duplicated the numbers. we want to watch out for this and drop.
same_zones = [
    "NS1", "EW24", "NS25", "EW13", "NS26", "EW14", "CC4", "DT15"
]
#similarly, sanity checks for JUR, SKG, PGL, ORC

same_zones_div_2 = [
    "EW24/NS1", "NE16/STC", "NE17/PTC"
]

# unpaid interchanges. we don't want to sum them since we can't tell who's changing and who's exiting.
unpaid_ints = [
    "BP6/DT1", "PB6/DT1", "EW2/DT32", "DT32/EW2", "NS21/DT11", "DT11/NS21"
]

# special handing for 202212 onwards - interchange stations were not summed
interchanges = {
    "CC10/DT26": ["CC10", "DT26"] ,
    "CC17/TE9": ["CC17", "TE9"] ,
    "CC19/DT9": ["CC19", "DT9"] ,
    "CC4/DT15": ["CC4", "DT15"] ,
    "CE1/DT16": ["CE1", "DT16"] ,
    "CG1/DT35": ["CG1", "DT35"] ,
    "DT10/TE11": ["DT10", "TE11"],
    "EW12/DT14": ["EW12", "DT14"] ,
    #"EW14/NS26": ["EW14", "NS26"] ,
    "EW16/NE3/TE17": ["EW16", "NE3", "TE17"],
    #"EW2/DT32": ["EW2", "DT32"] ,
    "EW21/CC22": ["EW21", "CC22"] ,
    "EW24/NS1": ["EW24", "NS1"] ,
    "EW8/CC9": ["EW8", "CC9"] ,
    "NE1/CC29": ["NE1", "CC29"] ,
    "NE12/CC13": ["NE12", "CC13"] ,
    "NE16/STC": ["NE16", "STC"] ,
    "NE17/PTC": ["NE17", "PTC"] ,
    "NE4/DT19": ["NE4", "DT19"] ,
    "NE7/DT12": ["NE7", "DT12"] ,
    "NS17/CC15": ["NS17", "CC15"] ,
    #"NS21/DT11": ["NS21", "DT11"] ,
    "NS24/NE6/CC1": ["NS24", "NE6", "CC1"],
    #"NS25/EW13": ["NS25", "EW13"] ,
    "NS27/CE2/TE20": ["NS27", "CE2", "TE20"],
    "NS4/BP1": ["NS4", "BP1"] ,
    "NS9/TE2": ["NS9", "TE2"] ,
    #"PB6/DT1": ["PB6", "DT1"] ,
    "TE14/NS22": ["TE14", "NS22"] ,

}

month = input("Dataset for (YYYYMM): ")
weekdays = input("Number of weekdays: ")
weekdays = int(weekdays)

specials = input("Number of weekends and holidays: ")
specials = int(specials)

if not os.path.exists(os.path.join(os.getcwd(), "processed_data", month)):
    os.mkdir(os.path.join(os.getcwd(), "processed_data", month))

total = specials + weekdays

in_file = os.path.join(os.getcwd(), "raw_data", month, "transport_node_train_" + month + ".csv")

df = pd.read_csv(in_file)

#special handling for 6 TEL interchanges
df_tel = df[df['PT_CODE'].isin(sum(interchanges.values(), []))]
for code, code_array in interchanges.items():
    df_telint = df[df['PT_CODE'].isin(code_array)]
    column_map = {col: "first" for col in df.columns}
    column_map["TOTAL_TAP_IN_VOLUME"] = "sum"
    column_map["TOTAL_TAP_OUT_VOLUME"] = "sum"
    df_telint2 = df_telint.groupby(["DAY_TYPE", "TIME_PER_HOUR"], as_index=False).aggregate(column_map)
    df_telint2["PT_CODE"] = code
    df = df[~df["PT_CODE"].isin(code_array)]
    df = df[~df["PT_CODE"].isin([code])]
    df_telint2.reset_index(drop=True, inplace=True)
    df = pd.concat([df, df_telint2])

df['multiplier'] = df['DAY_TYPE']

# not needed wef 202212
#df = df.replace({'PT_CODE': interchange_codes, 'multiplier': {'WEEKENDS/HOLIDAY': specials, 'WEEKDAY': weekdays}})
df = df.replace({'multiplier': {'WEEKENDS/HOLIDAY': specials, 'WEEKDAY': weekdays}})

#because for some reason we are given each node of the interchange AND the totals,
#drop the nodes and keep the totals.
#df = df[~df['PT_CODE'].isin(list(interchange_codes.items()))]
df = df[~df['PT_CODE'].isin(same_zones)]
df = df[~df['PT_CODE'].isin(unpaid_ints)]

df_filter = df[df['PT_CODE'].isin(same_zones_div_2)]
df = df[~df['PT_CODE'].isin(same_zones_div_2)]
df_filter['TOTAL_TAP_IN_VOLUME'] = (df_filter['TOTAL_TAP_IN_VOLUME'] / 2)
df_filter['TOTAL_TAP_OUT_VOLUME'] = (df_filter['TOTAL_TAP_OUT_VOLUME'] / 2)
df = pd.concat([df, df_filter])

df2 = df.groupby(['PT_CODE']).sum()
df2['TOTAL_TAP_IN_VOLUME'] = (df2['TOTAL_TAP_IN_VOLUME'] / total).round(1)
df2['TOTAL_TAP_OUT_VOLUME'] = (df2['TOTAL_TAP_OUT_VOLUME'] / total).round(1)

df['TOTAL_TAP_IN_VOLUME'] = (df['TOTAL_TAP_IN_VOLUME'] / df['multiplier']).round(1)
df['TOTAL_TAP_OUT_VOLUME'] = (df['TOTAL_TAP_OUT_VOLUME'] / df['multiplier']).round(1)

df['TOTAL_USAGE'] = df['TOTAL_TAP_IN_VOLUME'] + df['TOTAL_TAP_OUT_VOLUME']
df2['TOTAL_USAGE'] = df2['TOTAL_TAP_IN_VOLUME'] + df2['TOTAL_TAP_OUT_VOLUME']

df1 = df.drop(columns=['multiplier'])

df = df.groupby(['DAY_TYPE', 'PT_CODE']).agg({'TOTAL_TAP_IN_VOLUME': np.sum, 'TOTAL_TAP_OUT_VOLUME': np.sum, 'TOTAL_USAGE': np.sum, 'multiplier': "first"})
df = df.drop(columns=['multiplier'])

#new_index = pd.MultiIndex.from_frame(df[['PT_CODE', 'DAY_TYPE']])
df1 = pd.pivot_table(df1, index=['PT_CODE', 'DAY_TYPE'], columns=["TIME_PER_HOUR"], aggfunc={'TOTAL_TAP_IN_VOLUME': np.sum, 'TOTAL_TAP_OUT_VOLUME': np.sum, 'TOTAL_USAGE': np.sum}, fill_value=0)

df2 = df2.groupby(['PT_CODE']).agg({'TOTAL_TAP_IN_VOLUME': np.sum, 'TOTAL_TAP_OUT_VOLUME': np.sum, 'TOTAL_USAGE': np.sum})

#print (df['TOTAL_TAP_IN_VOLUME'].sum(), df['TOTAL_TAP_IN_VOLUME'].sum())
#df2 = df2.drop(columns=['TIME_PER_HOUR', 'multiplier'])

df.to_csv(os.path.join(os.getcwd(), "processed_data", month, "transport_node_train_" + month + "_summary_" + datetime.datetime.now().strftime('%Y%m%d%H%M%S') + ".csv"))
df1.to_csv(os.path.join(os.getcwd(), "processed_data", month, "transport_node_train_" + month + "_byhour_" + datetime.datetime.now().strftime('%Y%m%d%H%M%S') + ".csv"))
df2.to_csv(os.path.join(os.getcwd(), "processed_data", month, "transport_node_train_" + month + "_nodays_" + datetime.datetime.now().strftime('%Y%m%d%H%M%S') + ".csv"))

#now for orig dest

in_file = os.path.join(os.getcwd(), "raw_data", month, "origin_destination_train_" + month + ".csv")

df = pd.read_csv(in_file)
#special handling for 6 TEL interchanges
df_tel = df[df['ORIGIN_PT_CODE'].isin(sum(interchanges.values(), []))]
for code, code_array in interchanges.items():
    df_telint = df[df['ORIGIN_PT_CODE'].isin(code_array)]
    column_map = {col: "first" for col in df.columns}
    column_map["TOTAL_TRIPS"] = "sum"
    df_telint2 = df_telint.groupby(["DAY_TYPE", "TIME_PER_HOUR", "DESTINATION_PT_CODE"], as_index=False).aggregate(column_map)
    df_telint2["ORIGIN_PT_CODE"] = code
    df = df[~df["ORIGIN_PT_CODE"].isin(code_array)]
    df_telint2.reset_index(drop=True, inplace=True)
    df = pd.concat([df, df_telint2])

df_tel = df[df['DESTINATION_PT_CODE'].isin(sum(interchanges.values(), []))]
for code, code_array in interchanges.items():
    df_telint = df[df['DESTINATION_PT_CODE'].isin(code_array)]
    column_map = {col: "first" for col in df.columns}
    column_map["TOTAL_TRIPS"] = "sum"
    df_telint2 = df_telint.groupby(["DAY_TYPE", "TIME_PER_HOUR", "ORIGIN_PT_CODE"], as_index=False).aggregate(column_map)
    df_telint2["DESTINATION_PT_CODE"] = code
    df = df[~df["DESTINATION_PT_CODE"].isin(code_array)]
    df_telint2.reset_index(drop=True, inplace=True)
    df = pd.concat([df, df_telint2])

df['multiplier'] = df['DAY_TYPE']

# not needed wef 202212
#df = df.replace({'ORIGIN_PT_CODE': interchange_codes, 'DESTINATION_PT_CODE': interchange_codes, 'multiplier': {'WEEKENDS/HOLIDAY': specials, 'WEEKDAY': weekdays}})
df = df.replace({'multiplier': {'WEEKENDS/HOLIDAY': specials, 'WEEKDAY': weekdays}})

df = df[~df['ORIGIN_PT_CODE'].isin(same_zones)]
df = df[~df['ORIGIN_PT_CODE'].isin(unpaid_ints)]

df = df[~df['DESTINATION_PT_CODE'].isin(same_zones)]
df = df[~df['DESTINATION_PT_CODE'].isin(unpaid_ints)]

df_filter = df[df['ORIGIN_PT_CODE'].isin(same_zones_div_2)]
df = df[~df['ORIGIN_PT_CODE'].isin(same_zones_div_2)]
df_filter['TOTAL_TRIPS'] = (df_filter['TOTAL_TRIPS'] / 2)
df = pd.concat([df, df_filter])

df_filter = df[df['DESTINATION_PT_CODE'].isin(same_zones_div_2)]
df = df[~df['DESTINATION_PT_CODE'].isin(same_zones_div_2)]
df_filter['TOTAL_TRIPS'] = (df_filter['TOTAL_TRIPS'] / 2)
df = pd.concat([df, df_filter])

df2 = df.groupby(['ORIGIN_PT_CODE', 'DESTINATION_PT_CODE']).sum()
df2['TOTAL_TRIPS'] = (df2['TOTAL_TRIPS'] / total).round(1)

df['TOTAL_TRIPS'] = (df['TOTAL_TRIPS'] / df['multiplier']).round(1)
df = df[df['TOTAL_TRIPS'] !=0]
df1 = df.drop(columns=['multiplier'])

df = df.groupby(['DAY_TYPE', 'ORIGIN_PT_CODE', 'DESTINATION_PT_CODE']).agg({'TOTAL_TRIPS': np.sum, 'multiplier': "first"})
df = df.drop(columns=['multiplier'])
df2 = df2.drop(columns=['TIME_PER_HOUR', 'multiplier'])

df1 = pd.pivot_table(df1, index=['ORIGIN_PT_CODE', 'DESTINATION_PT_CODE', 'DAY_TYPE'], columns=["TIME_PER_HOUR"], aggfunc={'TOTAL_TRIPS': np.sum})

df.to_csv(os.path.join(os.getcwd(), "processed_data", month, "origin_destination_train_" + month + "_summary_" + datetime.datetime.now().strftime('%Y%m%d%H%M%S') + ".csv"))
df1.to_csv(os.path.join(os.getcwd(), "processed_data", month, "origin_destination_train_" + month + "_byhour_" + datetime.datetime.now().strftime('%Y%m%d%H%M%S') + ".csv"))
df2.to_csv(os.path.join(os.getcwd(), "processed_data", month, "origin_destination_train_" + month + "_nodays_" + datetime.datetime.now().strftime('%Y%m%d%H%M%S') + ".csv"))

