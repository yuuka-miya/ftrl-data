import pandas as pd
import numpy as np
import os

df = pd.DataFrame()

# if os.path.exists("processed_data/summary.csv"):
    # df_in = pd.read_csv("processed_data/summary.csv", header=[0,1])
    # df_in = df_in.drop([0])
    # temp = df_in.columns.to_numpy()
    # temp[0] = ('DAY_TYPE', None)
    # temp[1] = ('PT_CODE', None)
    # df_in.columns = pd.MultiIndex.from_tuples(temp)
    # #print(df_in.columns)
    # df_in.columns.set_names("month", level = 1)

    # df_in = df_in.set_index([ ('DAY_TYPE', None), ('PT_CODE', None)])
    # df_in.index.set_names("DAY_TYPE" , level=0)
    # df_in.index.set_names("PT_CODE" , level=1)

for subdir, dirs, files in os.walk("processed_data"):
    for file in files:
        #print os.path.join(subdir, file)
        filepath = os.path.join(subdir,file)

        if "_wholemonth_" in filepath:
            if "node" in filepath:
                df_plus = pd.read_csv(filepath)
                month_tag = os.path.split(subdir)[1]
                df_plus.insert(0, "month", month_tag)
                df = df.append(df_plus, ignore_index=True, sort=False)
                # if ('TOTAL_TAP_IN_VOLUME', month_tag) in df_in.columns:
                    # print(month_tag)

dft = df['PT_CODE'].str.split('/').str[0]
dft = dft.str.split('-').str[0]
#dft.to_csv("test.csv")

df['PT_CODE'] = dft
#print(dft)                
df1 = pd.pivot_table(df, index=["DAY_TYPE", 'PT_CODE'], columns=["month"], aggfunc={'TOTAL_TAP_IN_VOLUME': np.sum, 'TOTAL_TAP_OUT_VOLUME': np.sum})

#df1 = pd.concat([df1, df_in])
df1 = df1.groupby(["DAY_TYPE", 'PT_CODE']).sum()
df1.to_csv("processed_data/summary.csv")

# if os.path.exists("processed_data/odsummary.csv"):
    # df_in = pd.read_csv("processed_data/odsummary.csv", header=[0,1])
    # df_in = df_in.drop([0])
    # temp = df_in.columns.to_numpy()
    # temp[0] = ('DAY_TYPE', None)
    # temp[1] = ('ORIGIN_PT_CODE', None)
    # temp[2] = ('DESTINATION_PT_CODE', None)
    # df_in.columns = pd.MultiIndex.from_tuples(temp)

    # df_in.columns.set_names("month", level = 1)

    # df_in = df_in.set_index([ ('DAY_TYPE', None), ('ORIGIN_PT_CODE', None), ('DESTINATION_PT_CODE', None)])
    # df_in.index.set_names("DAY_TYPE" , level=0)
    # df_in.index.set_names("ORIGIN_PT_CODE" , level=1)
    # df_in.index.set_names("DESTINATION_PT_CODE" , level=2)

for subdir, dirs, files in os.walk("processed_data"):
    for file in files:
        filepath = os.path.join(subdir,file)

        if "_wholemonth_" in filepath:
            if "origin" in filepath:
                df_plus = pd.read_csv(filepath)
                month_tag = os.path.split(subdir)[1]
                df_plus = df_plus[df_plus['TOTAL_TRIPS'] !=0]
                df_plus.insert(0, "month", month_tag)
                df = df.append(df_plus, ignore_index=True, sort=False)
                # if ('TOTAL_TRIPS', month_tag) in df_in.columns:
                    # print(month_tag)

df1 = pd.pivot_table(df, index=["DAY_TYPE", 'ORIGIN_PT_CODE', 'DESTINATION_PT_CODE'], columns=["month"], aggfunc={'TOTAL_TRIPS': np.sum})

#df1 = pd.concat([df1, df_in])
df1 = df1.groupby(["DAY_TYPE", 'ORIGIN_PT_CODE', 'DESTINATION_PT_CODE']).sum()
df1.to_csv("processed_data/odsummary.csv")