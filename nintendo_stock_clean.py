import time
start_time = time.time()
import pandas as pd
from datetime import datetime

def cleanNintendoStock(stock_data):
    #reading in the data into a pandas df
    nintendoStock = pd.read_csv(stock_data)

    #initial cleaning (keeping important features/columns)
    nintendoStock = nintendoStock.loc[:,['Date', 'Close', 'Volume']]
    #removing first row because there is only 1 datapoint for the first week

    #converting date values all into week format
    nintendoStock['Date'] = pd.to_datetime(nintendoStock['Date'])
    nintendoStock['week'] = nintendoStock['Date'].dt.to_period(freq='W-SUN')

    #creating new table to group each week by the sum of the volume for that week
    sum_volume_nintendoData = nintendoStock.groupby(['week'])[['Volume']].sum().reset_index()
    sum_volume_nintendoData = sum_volume_nintendoData.rename(columns={'Volume': 'weeklyVol'})

    #calculatng and using the average weekly volume to be a threshold
    averageVol = sum_volume_nintendoData['weeklyVol'].mean()

    #create a new column representing if a week has an above average volume (1) or below average (0)
    sum_volume_nintendoData['aboveAvgVol'] = sum_volume_nintendoData['weeklyVol'].apply(lambda x: 1 if x > averageVol else 0)
    #we no longer need our summed volumes, as we only require 0 and 1 values for our pytorch use
    sum_volume_nintendoData = sum_volume_nintendoData.drop('weeklyVol', axis=1)

    #moving on to the close data
    #this code block essentially calculates the percent change from the beginning of the week to the end of the week
    #For example, if monday closes at $4 and friday closes at $6, that week will be a single row containing 0.33333 due to a 33% change
    weeks = nintendoStock['week'].unique()
    percentChanges = []
    for week in weeks:
        weekData = nintendoStock.loc[nintendoStock['week'] == week, 'Close']
        percentChange = (weekData.iloc[-1] - weekData.iloc[0]) / weekData.iloc[0]
        percentChanges.append(abs(percentChange))
    percentChange_nintendoData = pd.DataFrame({'week': weeks, 'percentChange': percentChanges})

    #creating a new column representing if a week has a high (above 6% change) (1) or below 6%, (0)
    percentChange_nintendoData['highChange (above 6% change)'] = percentChange_nintendoData['percentChange'].apply(lambda x: 1 if x > 0.06 else 0)
    #we no longer need our percent changes for each week, as we only require 0 and 1 values for our pytorch use
    percentChange_nintendoData = percentChange_nintendoData.drop('percentChange', axis=1)

    #our final, clean, merged stock data for GoPro
    nintendo_cleaned_stock = percentChange_nintendoData.merge(sum_volume_nintendoData, on='week')

    print(nintendo_cleaned_stock)

    end_time = time.time()
    print("Execution time: {:.2f} seconds".format(end_time - start_time))

cleanNintendoStock('nintendo_stock.csv')