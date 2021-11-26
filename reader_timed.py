import pandas as pd
import numpy as np
import os
import datetime

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

in_file = os.path.join(os.getcwd(), "raw_data", month, "transport_node_train_" + month + ".csv")

df = pd.read_csv(in_file)
df['multiplier'] = df['DAY_TYPE']
df = df.replace({'PT_CODE': interchange_codes, 'multiplier': {'WEEKENDS/HOLIDAY': specials, 'WEEKDAY': weekdays}})

df['TOTAL_TAP_IN_VOLUME'] = (df['TOTAL_TAP_IN_VOLUME'] / df['multiplier']).round(1)
df['TOTAL_TAP_OUT_VOLUME'] = (df['TOTAL_TAP_OUT_VOLUME'] / df['multiplier']).round(1)

df['TOTAL_USAGE'] = df['TOTAL_TAP_IN_VOLUME'] + df['TOTAL_TAP_OUT_VOLUME']

df1 = df.drop(columns=['multiplier'])
df = df.groupby(['DAY_TYPE', 'PT_CODE']).agg({'TOTAL_TAP_IN_VOLUME': np.sum, 'TOTAL_TAP_OUT_VOLUME': np.sum, 'TOTAL_USAGE': np.sum, 'multiplier': "first"})
df = df.drop(columns=['multiplier'])

#new_index = pd.MultiIndex.from_frame(df[['PT_CODE', 'DAY_TYPE']])
df1 = pd.pivot_table(df1, index=['PT_CODE', 'DAY_TYPE'], columns=["TIME_PER_HOUR"], aggfunc={'TOTAL_TAP_IN_VOLUME': np.sum, 'TOTAL_TAP_OUT_VOLUME': np.sum, 'TOTAL_USAGE': np.sum}, fill_value=0)

#print (df['TOTAL_TAP_IN_VOLUME'].sum(), df['TOTAL_TAP_IN_VOLUME'].sum())

df.to_csv(os.path.join(os.getcwd(), "processed_data", month, "transport_node_train_" + month + "_summary_" + datetime.datetime.now().strftime('%Y%m%d%H%M%S') + ".csv"))
df1.to_csv(os.path.join(os.getcwd(), "processed_data", month, "transport_node_train_" + month + "_byhour_" + datetime.datetime.now().strftime('%Y%m%d%H%M%S') + ".csv"))

#now for orig dest

in_file = os.path.join(os.getcwd(), "raw_data", month, "origin_destination_train_" + month + ".csv")

df = pd.read_csv(in_file)

df['multiplier'] = df['DAY_TYPE']

df = df.replace({'ORIGIN_PT_CODE': interchange_codes, 'DESTINATION_PT_CODE': interchange_codes, 'multiplier': {'WEEKENDS/HOLIDAY': specials, 'WEEKDAY': weekdays}})
df['TOTAL_TRIPS'] = (df['TOTAL_TRIPS'] / df['multiplier']).round(1)
df = df[df['TOTAL_TRIPS'] !=0]
df1 = df.drop(columns=['multiplier'])

df = df.groupby(['DAY_TYPE', 'ORIGIN_PT_CODE', 'DESTINATION_PT_CODE']).agg({'TOTAL_TRIPS': np.sum, 'multiplier': "first"})
df = df.drop(columns=['multiplier'])

df1 = pd.pivot_table(df1, index=['ORIGIN_PT_CODE', 'DESTINATION_PT_CODE', 'DAY_TYPE'], columns=["TIME_PER_HOUR"], aggfunc={'TOTAL_TRIPS': np.sum})

df.to_csv(os.path.join(os.getcwd(), "processed_data", month, "origin_destination_train_" + month + "_summary_" + datetime.datetime.now().strftime('%Y%m%d%H%M%S') + ".csv"))
df1.to_csv(os.path.join(os.getcwd(), "processed_data", month, "origin_destination_train_" + month + "_byhour_" + datetime.datetime.now().strftime('%Y%m%d%H%M%S') + ".csv"))
