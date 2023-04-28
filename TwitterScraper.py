import pandas as pd
from datetime import datetime
#GoPro Data
GoPro = "Twitter_GoPro.csv"
Tesla = "Twitter_Tesla.csv"
Nintendo = "Twitter_Nintendo.csv"
elonMusk = "Twitter_elonmusk.csv"

#Function that processes any twitter dataset in a csv format from apify and returns a clean fataframe
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

df1 = TwitterClean(GoPro)
df2 = TwitterClean(Tesla)
df3 = TwitterClean(Nintendo)
df4 = TwitterClean(elonMusk)