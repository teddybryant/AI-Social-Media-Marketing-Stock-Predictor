import time
start_time = time.time()
import pandas as pd
from datetime import datetime
import numpy as np

#Reading in the data
df = pd.read_csv(r'C:\Users\ved\Documents\DSS Final Project\GoPro TikTok Data.csv', encoding='utf-8')

#Filtering data to only show required columns
df = df.loc[:, 
['commentCount',
 'createTimeISO',
 'playCount',
 'shareCount',
 'diggCount']]

#renaming the columns that do not have user friendly names
df = df.rename(columns={'createTimeISO': 'datePosted', 'diggCount':'likeCount'})

#converting all timestamps into date time
def convert_date(date_string):
    datetime_obj = datetime.strptime(date_string, '%Y-%m-%dT%H:%M:%S.%fZ')
    formatted_date = datetime_obj.strftime('%m/%d/%Y')
    return formatted_date
df['datePosted']=df['datePosted'].apply(convert_date)

#sorting dates in ascending order
df.sort_values(by='datePosted', key=lambda x: pd.to_datetime(x, format='%m/%d/%Y'), inplace=True)

#converting our datePosted column to datetime, so we can group each date by specific weeks
#we will later use these weeks to have sums as well as averages for social media outcomes that week
df['datePosted'] = pd.to_datetime(df['datePosted'])
df['week'] = df['datePosted'].dt.to_period(freq='W-SUN')

#creating a dataframe that computes a sum for all post metrics during that week
sum_weekly_data = df.groupby(['week'])[['likeCount', 'playCount', 'shareCount', 'commentCount']].sum().reset_index()
sum_weekly_data = sum_weekly_data.rename(columns={
'playCount': 'playCountSum', 
'commentCount':'commentCountSum', 
'shareCount': 'shareCountSum', 
'likeCount':'likeCountSum'})

#creating a dataframe that computes a mean/ average for all post metrics during that week
mean_weekly_data = df.groupby(['week'])[['likeCount', 'playCount', 'shareCount', 'commentCount']].mean().reset_index()
mean_weekly_data = mean_weekly_data.rename(columns={
    'playCount': 'playCountAvg', 
    'commentCount':'commentCountAvg', 
    'shareCount': 'shareCountAvg', 
    'likeCount':'likeCountAvg'})

#merging our mean and sum data for each week
gopro_tiktok_wkly_data = sum_weekly_data.merge(mean_weekly_data, on='week')

print(gopro_tiktok_wkly_data)

end_time = time.time()
print("Execution time: {:.2f} seconds".format(end_time - start_time))