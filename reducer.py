#!/usr/bin/env python3

import sys
import json
import pandas as pd

def reducer(file2):
    input_data = [json.loads(line) for line in sys.stdin]
    df_over_avg = pd.DataFrame(input_data)
    
    # read in the CSV file as a DataFrame
    df2 = pd.read_csv(file2)

    avg_volume = df2['Volume'].mean()

    # create a new DataFrame with the date, average price, and volume columns
    stock_df = pd.DataFrame({'Date': df2['Date'],
                             'Volume': df2['Volume'],
                             'Change_Price': abs(((df2['Close'] - df2['Open']) / df2['Open']) * 10000),
                             'Change_Volume': abs(((df2['Volume'] - avg_volume) / avg_volume) * 100)})

    merged_df = pd.merge(df_over_avg, stock_df, on='Date')

    # Output the data in JSON format
    for index, row in merged_df.iterrows():
        print(json.dumps(row.to_dict()))

if __name__ == "__main__":
    # Call the reducer function
    reducer(sys.argv[1])
