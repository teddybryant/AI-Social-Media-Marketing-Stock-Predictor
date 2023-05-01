import time
import pandas as pd
from datetime import datetime


def CleanInstagram(instagram_data):
    #Reading in the data
    df = pd.read_csv(instagram_data)

    #Filtering data to only show required columns
    df = df.loc[:, 
    ['timestamp',
    'likesCount',
    'commentsCount']]

    #renaming the columns that do not have user friendly names
    df = df.rename(columns={'timestamp': 'datePosted'})

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
    sum_weekly_data = df.groupby(['week'])[['likesCount','commentsCount']].sum().reset_index()
    sum_weekly_data = sum_weekly_data.rename(columns={ 
        'commentsCount':'instaCommentCountSum',  
        'likesCount':'instaLkeCountSum'})

    #creating a dataframe that computes a mean/ average for all post metrics during that week
    mean_weekly_data = df.groupby(['week'])[['likesCount','commentsCount']].mean().reset_index()
    mean_weekly_data = mean_weekly_data.rename(columns={
        'commentsCount':'instaCommentCountAvg', 
        'likesCount':'instaLikeCountAvg'})

    #merging our mean and sum data for each week
    insta_wkly_data = sum_weekly_data.merge(mean_weekly_data, on='week')

    return insta_wkly_data

def CleanStock(stock_data):
    #reading in the data into a pandas df
    stock = pd.read_csv(stock_data)

    #initial cleaning (keeping important features/columns)
    stock = stock.loc[:,['Date', 'Close', 'Volume']]
    #removing first row because there is only 1 datapoint for the first week
    if(stock_data == 'tesla_stock.csv' or stock_data == 'gopro_stock.csv'):
        stock = stock.drop(stock.index[0])
    #converting date values all into week format
    stock['Date'] = pd.to_datetime(stock['Date'])
    stock['week'] = stock['Date'].dt.to_period(freq='W-SUN')

    #creating new table to group each week by the sum of the volume for that week
    sumVolume = stock.groupby(['week'])[['Volume']].sum().reset_index()
    sumVolume = sumVolume.rename(columns={'Volume': 'weeklyVol'})

    #calculatng and using the average weekly volume to be a threshold
    averageVol = sumVolume['weeklyVol'].mean()

    #create a new column representing if a week has an above average volume (1) or below average (0)
    sumVolume['aboveAvgVol'] = sumVolume['weeklyVol'].apply(lambda x: 1 if x > averageVol else 0)
    #we no longer need our summed volumes, as we only require 0 and 1 values for our pytorch use
    sumVolume = sumVolume.drop('weeklyVol', axis=1)

    #moving on to the close data
    #this code block essentially calculates the percent change from the beginning of the week to the end of the week
    #For example, if monday closes at $4 and friday closes at $6, that week will be a single row containing 0.33333 due to a 33% change
    weeks = stock['week'].unique()
    percentChanges = []
    for week in weeks:
        weekData = stock.loc[stock['week'] == week, 'Close']
        percentChange = (weekData.iloc[-1] - weekData.iloc[0]) / weekData.iloc[0]
        percentChanges.append(abs(percentChange))
    pctChange = pd.DataFrame({'week': weeks, 'percentChange': percentChanges})

    #creating a new column representing if a week has a high (above 6% change) (1) or below 6%, (0)
    pctChange['highChange (above 6% change)'] = pctChange['percentChange'].apply(lambda x: 1 if x > 0.06 else 0)
    #we no longer need our percent changes for each week, as we only require 0 and 1 values for our pytorch use
    pctChange = pctChange.drop('percentChange', axis=1)

    #our final, clean, merged stock data for GoPro
    nintendo_cleaned_stock = pctChange.merge(sumVolume, on='week')
    return nintendo_cleaned_stock

def TwitterClean(file):   
    df = pd.read_csv(file)
    dateList = []
    df.fillna(0)
    #Changing string date to datatime date
    for i in range(len(df.index)):
        s = df.iloc[i,5][:10]
        date_time_obj = datetime.strptime(s, '%Y-%m-%d')
        dateList.append(date_time_obj)
    
    #Grouping all the data by week instead of the date
    df["time_stamp"] = dateList
    df = df.drop(columns=['created_at'])
    dates = df.loc[:,"time_stamp"]
    d ={"Twitter_Week":[],"Twitter_View_Count":0, "Twitter_Reply_Count":0, "Twitter_Retweet_Count":0, "Twitter_Favorite_Count:0, Twitter_Quote_Count":0}
    initial_date = max(dates)
    df = df.sort_values(by=['time_stamp'], ascending = False)
    df = df.fillna(0)
    df['time_stamp'] = pd.to_datetime(df['time_stamp'])
    df['week'] = df['time_stamp'].dt.to_period(freq='W-SUN')

    #Finding the sum of all parameters and creating a new dataframe
    sum_weekly_data = df.groupby(['week'])[['view_count', 'reply_count', 'retweet_count', 'favorite_count','quote_count']].sum().reset_index()
    sum_weekly_data = sum_weekly_data.rename(columns={
    'view_count': 'twitterViewCountSum', 
    'reply_count':'twitterReplyCountSum', 
    'retweet_count': 'twitterRetweetCountSum', 
    'favorite_count':'twitterLikeCountSum',
    'quote_count':'twitterQuoteCountSum'})

    #Finding the mean of all parameters and creating a new dataframe
    mean_weekly_data = df.groupby(['week'])[['view_count', 'reply_count', 'retweet_count', 'favorite_count','quote_count']].mean().reset_index()
    mean_weekly_data = mean_weekly_data.rename(columns={
    'view_count': 'twitterViewCountMean', 
    'reply_count':'twitterReplyCountMean', 
    'retweet_count': 'twitterRetweetCountMean', 
    'favorite_count':'twitterLikeCountMean',
    'quote_count':'twitterQuoteCountMean'})
    gopro_twitter_wkly_data = sum_weekly_data.merge(mean_weekly_data, on='week')
    return gopro_twitter_wkly_data

def TikTokClean(file):
    df = pd.read_csv(file)

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
    'playCount': 'ttPlayCountSum', 
    'commentCount':'ttCommentCountSum', 
    'shareCount': 'ttShareCountSum', 
    'likeCount':'ttLikeCountSum'})

    #creating a dataframe that computes a mean/ average for all post metrics during that week
    mean_weekly_data = df.groupby(['week'])[['likeCount', 'playCount', 'shareCount', 'commentCount']].mean().reset_index()
    mean_weekly_data = mean_weekly_data.rename(columns={
        'playCount': 'ttPlayCountAvg', 
        'commentCount':'ttCommentCountAvg', 
        'shareCount': 'ttShareCountAvg', 
        'likeCount':'ttLikeCountAvg'})

    #merging our mean and sum data for each week
    gopro_tiktok_wkly_data = sum_weekly_data.merge(mean_weekly_data, on='week')
    return gopro_tiktok_wkly_data


def main():
    start_time = time.time()
    nintendo_insta_df = CleanInstagram('nintendo_instagram.csv')
    nintendo_cleaned_stock = CleanStock('nintendo_stock.csv')
    nintendo_twitter_df = TwitterClean('Twitter_Nintendo.csv')

    nintendo_final_df = nintendo_twitter_df.merge(nintendo_insta_df,on='week').merge(nintendo_cleaned_stock, on='week')
    print(nintendo_final_df)
    nintendo_final_df.to_csv('nintendo_final_data.csv', index=False)

    tesla_insta_df = CleanInstagram('tesla_instagram.csv')
    tesla_cleaned_stock = CleanStock('tesla_stock.csv')
    tesla_twitter_df = TwitterClean('Twitter_TeslaElon.csv')

    tesla_final_df = tesla_twitter_df.merge(tesla_insta_df,on='week').merge(tesla_cleaned_stock, on='week')
    print(tesla_final_df)
    tesla_final_df.to_csv('tesla_final_data.csv', index=False)


    gopro_insta_df = CleanInstagram('gopro_instagram.csv')
    gopro_cleaned_stock = CleanStock('tesla_stock.csv')
    gopro_twitter_df = TwitterClean('Twitter_GoPro.csv')
    gopro_tiktok_df = TikTokClean('GoPro_TikTok_Data.csv')

    gopro_final_df = gopro_twitter_df.merge(gopro_insta_df,on='week').merge(gopro_tiktok_df, on='week').merge(gopro_cleaned_stock, on='week')
    print(gopro_final_df)
    gopro_final_df.to_csv('gopro_final_data.csv', index=False)


    end_time = time.time()
    print("Execution time: {:.2f} seconds".format(end_time - start_time))

if __name__ == '__main__':
    main()