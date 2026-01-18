import cudf.pandas
cudf.pandas.install()

import pandas as pd
import numpy as np
import os
import datetime

month = input("Dataset for (YYYYMM): ")
weekdays = input("Number of weekdays: ")
weekdays = int(weekdays)

specials = input("Number of weekends and holidays: ")
specials = int(specials)

if not os.path.exists(os.path.join(os.getcwd(), "processed_data", month)):
    os.mkdir(os.path.join(os.getcwd(), "processed_data", month))

total = specials + weekdays

in_file = os.path.join(os.getcwd(), "raw_data", month, "transport_node_bus_" + month + ".csv")

df = pd.read_csv(in_file)
df['multiplier'] = df['DAY_TYPE']
df = df.replace({'multiplier': {'WEEKENDS/HOLIDAY': specials, 'WEEKDAY': weekdays}})

df2 = df.groupby(['PT_CODE']).agg("sum")
df2['TOTAL_TAP_IN_VOLUME'] = (df2['TOTAL_TAP_IN_VOLUME'] / total).round(0)
df2['TOTAL_TAP_OUT_VOLUME'] = (df2['TOTAL_TAP_OUT_VOLUME'] / total).round(0)

df['TOTAL_TAP_IN_VOLUME'] = (df['TOTAL_TAP_IN_VOLUME'] / df['multiplier']).round(0)
df['TOTAL_TAP_OUT_VOLUME'] = (df['TOTAL_TAP_OUT_VOLUME'] / df['multiplier']).round(0)

df['TOTAL_USAGE'] = df['TOTAL_TAP_IN_VOLUME'] + df['TOTAL_TAP_OUT_VOLUME']
df2['TOTAL_USAGE'] = df2['TOTAL_TAP_IN_VOLUME'] + df2['TOTAL_TAP_OUT_VOLUME']

df1 = df.drop(columns=['multiplier'])
df = df.groupby(['DAY_TYPE', 'PT_CODE']).agg({'TOTAL_TAP_IN_VOLUME': "sum", 'TOTAL_TAP_OUT_VOLUME': "sum", 'TOTAL_USAGE': "sum", 'multiplier': "first"})
df = df.drop(columns=['multiplier'])

#new_index = pd.MultiIndex.from_frame(df[['PT_CODE', 'DAY_TYPE']])
df1 = pd.pivot_table(df1, index=['PT_CODE', 'DAY_TYPE'], columns=["TIME_PER_HOUR"], aggfunc={'TOTAL_TAP_IN_VOLUME': "sum", 'TOTAL_USAGE': "sum", 'TOTAL_TAP_OUT_VOLUME': "sum"}, fill_value=0)

df2 = df2.drop(columns=['DAY_TYPE', 'YEAR_MONTH', 'PT_TYPE', 'TIME_PER_HOUR', 'multiplier'])

df.to_csv(os.path.join(os.getcwd(), "processed_data", month, "transport_node_bus_" + month + "_summary_" + datetime.datetime.now().strftime('%Y%m%d%H%M%S') + ".csv"))
df1.to_csv(os.path.join(os.getcwd(), "processed_data", month, "transport_node_bus_" + month + "_byhour_" + datetime.datetime.now().strftime('%Y%m%d%H%M%S') + ".csv"))
df2.to_csv(os.path.join(os.getcwd(), "processed_data", month, "transport_node_bus_" + month + "_nodays_" + datetime.datetime.now().strftime('%Y%m%d%H%M%S') + ".csv"))

#now for orig dest

in_file = os.path.join(os.getcwd(), "raw_data", month, "origin_destination_bus_" + month + ".csv")

df = pd.read_csv(in_file)

df['multiplier'] = df['DAY_TYPE']

df = df.replace({'multiplier': {'WEEKENDS/HOLIDAY': specials, 'WEEKDAY': weekdays}})

df2 = df.groupby(['ORIGIN_PT_CODE', 'DESTINATION_PT_CODE']).agg("sum")
df2['TOTAL_TRIPS'] = (df2['TOTAL_TRIPS'] / total).round(1)

df['TOTAL_TRIPS'] = (df['TOTAL_TRIPS'] / df['multiplier']).round(1)
df = df[df['TOTAL_TRIPS'] !=0]
df1 = df.drop(columns=['multiplier'])
df2 = df2.drop(columns=['DAY_TYPE', 'YEAR_MONTH', 'PT_TYPE', 'TIME_PER_HOUR', 'multiplier'])

df = df.groupby(['DAY_TYPE', 'ORIGIN_PT_CODE', 'DESTINATION_PT_CODE']).agg({'TOTAL_TRIPS': "sum", 'multiplier': "first"})
df = df.drop(columns=['multiplier'])

df1 = pd.pivot_table(df1, index=['ORIGIN_PT_CODE', 'DESTINATION_PT_CODE', 'DAY_TYPE'], columns=["TIME_PER_HOUR"], aggfunc={'TOTAL_TRIPS': "sum"})

df.to_csv(os.path.join(os.getcwd(), "processed_data", month, "origin_destination_bus_" + month + "_summary_" + datetime.datetime.now().strftime('%Y%m%d%H%M%S') + ".csv"))
df1.to_csv(os.path.join(os.getcwd(), "processed_data", month, "origin_destination_bus_" + month + "_byhour_" + datetime.datetime.now().strftime('%Y%m%d%H%M%S') + ".csv"))
df2.to_csv(os.path.join(os.getcwd(), "processed_data", month, "origin_destination_bus_" + month + "_nodays_" + datetime.datetime.now().strftime('%Y%m%d%H%M%S') + ".csv"))