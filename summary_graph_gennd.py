import pandas as pd
import numpy as np
import os

year = input("Year: ")

selected = [
    "202307",
    "202308",
    "202309",
    "202310",
    "202311",
    "202312",
    "202401",
    "202402",
    "202403",
    "202404"
    
]

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

        if "_nodays_" in filepath:
            if "origin" in filepath:
                df_plus = pd.read_csv(filepath)
                month_tag = os.path.split(subdir)[1]
                if month_tag in selected:
                    df_plus.insert(0, "month", month_tag)
                    df = pd.concat([df, df_plus])
                # if ('TOTAL_TAP_IN_VOLUME', month_tag) in df_in.columns:
                    # print(month_tag)
df1 = pd.pivot_table(df, index=['ORIGIN_PT_CODE', 'DESTINATION_PT_CODE'], columns=["month"], aggfunc={'TOTAL_TRIPS': np.sum})

#df1 = pd.concat([df1, df_in])
df1 = df1.groupby(['ORIGIN_PT_CODE', 'DESTINATION_PT_CODE']).sum()
#df1.to_csv("processed_data/summary2.csv")
df2 = df1.sum()['TOTAL_TRIPS'].plot(kind="bar")
df2.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc='lower left',
           ncol=6, mode="expand", fontsize='xx-small')
df2.tick_params(labelrotation=0)
df2.get_figure().savefig(f"graph_in_{year}_nd.png", format='png')