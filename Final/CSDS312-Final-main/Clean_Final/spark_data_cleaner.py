
import time
start_time = time.time()
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, weekofyear, year, sum, avg, to_date, date_format, date_sub, date_trunc, date_add, when, lead, split, from_utc_timestamp, first, last, mean, dayofweek, lit
from pyspark.sql.window import Window
from pyspark.sql.types import DateType, TimestampType
from pyspark.sql import functions as F
spark = SparkSession.builder.appName("InstagramDataCleaner").getOrCreate()

def CleanInstagram(instagram_data):
    #Reading in the data
    df = spark.read.csv(instagram_data, header=True)

    #Filtering data to only show required columns
    df = df.select('timestamp', 'likesCount', 'commentsCount')
    
    #renaming the columns that do not have user friendly names
    df = df.withColumnRenamed('timestamp', 'datePosted')
    
    #converting all timestamps into date time
    df = df.withColumn('datePosted', to_date('datePosted', 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\''))

    #sorting dates in ascending order
    df = df.orderBy('datePosted')
    
    #converting our datePosted column to datetime, so we can group each date by specific weeks
    #we will later use these weeks to have sums as well as averages for social media outcomes that week
    df = df.withColumn('week', date_trunc('week', 'datePosted'))
    df = df.withColumn('week_start', date_format(col('week'), 'yyyy-MM-dd'))
    df = df.withColumn('week_end', date_format(date_add(col('week'), 6), 'yyyy-MM-dd'))
    
    #creating a dataframe that computes a sum for all post metrics during that week
    sum_weekly_data = df.groupby('week', 'week_start', 'week_end').agg({'likesCount': 'sum', 'commentsCount': 'sum'})
    sum_weekly_data = sum_weekly_data.withColumnRenamed('sum(likesCount)', 'instaLkeCountSum')
    sum_weekly_data = sum_weekly_data.withColumnRenamed('sum(commentsCount)', 'instaCommentCountSum')
    
    #creating a dataframe that computes a mean/ average for all post metrics during that week
    mean_weekly_data = df.groupby('week', 'week_start', 'week_end').agg({'likesCount': 'avg', 'commentsCount': 'avg'})
    mean_weekly_data = mean_weekly_data.withColumnRenamed('avg(likesCount)', 'instaLikeCountAvg')
    mean_weekly_data = mean_weekly_data.withColumnRenamed('avg(commentsCount)', 'instaCommentCountAvg')
    
    #merging our mean and sum data for each week
    insta_wkly_data = sum_weekly_data.join(mean_weekly_data, ['week', 'week_start', 'week_end'])
    
    return insta_wkly_data

#Clean stock function

def cleanStock(stock_data):
    # Reading in the data into a Spark DataFrame
    stock = spark.read.csv(stock_data, header=True)

    # Initial cleaning (keeping important features/columns)
    stock = stock.select('Date', 'Close', 'Volume')

    # Converting date values all into week format
    stock = stock.withColumn('Date', to_date('Date', 'yyyy-MM-dd'))
    stock = stock.withColumn('week', date_trunc('week', 'Date'))
    stock = stock.withColumn('week_start', date_format(col('week'), 'yyyy-MM-dd'))
    stock = stock.withColumn('week_end', date_format(date_add(col('week'), 6), 'yyyy-MM-dd'))

    # Creating new DataFrame to group each week by the sum of the volume for that week
    sumVolume = stock.groupBy('week', 'week_start', 'week_end').agg({'Volume': 'sum'})
    sumVolume = sumVolume.withColumnRenamed('sum(Volume)', 'weeklyVol')

    # Calculating and using the average weekly volume to be a threshold
    averageVol = sumVolume.select(avg(col('weeklyVol'))).collect()[0][0]

    # Creating a new column representing if a week has an above-average volume (1) or below average (0)
    sumVolume = sumVolume.withColumn('aboveAvgVol', when(col('weeklyVol') > averageVol, 1).otherwise(0))

    # Calculating percent changes from the beginning of the week to the end of the week
    stock = stock.withColumn('Close', col('Close').cast('double'))
    stock = stock.withColumn('nextClose', lead(stock['Close'], 1).over(Window.partitionBy('week').orderBy('Date')))
    stock = stock.withColumn('percentChange', (col('nextClose') - col('Close')) / col('Close'))
    stock = stock.groupBy('week', 'week_start', 'week_end').agg({'percentChange': 'sum'})

    # Creating a new column representing if a week has a high (above 6% change) (1) or below 6%, (0)
    stock = stock.withColumn('highChange (above 6% change)', when(col('sum(percentChange)') > 0.06, 1).otherwise(0))

    # Merging the data
    cleaned_stock = stock.join(sumVolume.select('week', 'aboveAvgVol'), 'week')

    return cleaned_stock.select('week', 'week_start', 'week_end', 'highChange (above 6% change)', 'aboveAvgVol')

def TwitterClean(file):
    # Reading in the data into a Spark DataFrame
    df = spark.read.csv(file, header=True)

    # Filling null values with 0
    df = df.na.fill(0)

    # Extracting the date from the 'created_at' column
    df = df.withColumn('time_stamp', from_utc_timestamp(df['created_at'], "UTC").cast(DateType()))

    # Dropping the 'created_at' column
    df = df.drop('created_at')

    # Grouping all the data by week instead of the date
    df = df.withColumn('week', date_trunc('week', 'time_stamp'))

    # Finding the sum of all parameters and creating a new DataFrame
    sum_weekly_data = df.groupBy('week').agg({
        'view_count': 'sum',
        'reply_count': 'sum',
        'retweet_count': 'sum',
        'favorite_count': 'sum',
        'quote_count': 'sum'
    }).withColumnRenamed('sum(view_count)', 'twitterViewCountSum') \
      .withColumnRenamed('sum(reply_count)', 'twitterReplyCountSum') \
      .withColumnRenamed('sum(retweet_count)', 'twitterRetweetCountSum') \
      .withColumnRenamed('sum(favorite_count)', 'twitterLikeCountSum') \
      .withColumnRenamed('sum(quote_count)', 'twitterQuoteCountSum')

    # Finding the mean of all parameters and creating a new DataFrame
    mean_weekly_data = df.groupBy('week').agg({
        'view_count': 'avg',
        'reply_count': 'avg',
        'retweet_count': 'avg',
        'favorite_count': 'avg',
        'quote_count': 'avg'
    }).withColumnRenamed('avg(view_count)', 'twitterViewCountMean') \
      .withColumnRenamed('avg(reply_count)', 'twitterReplyCountMean') \
      .withColumnRenamed('avg(retweet_count)', 'twitterRetweetCountMean') \
      .withColumnRenamed('avg(favorite_count)', 'twitterLikeCountMean') \
      .withColumnRenamed('avg(quote_count)', 'twitterQuoteCountMean')

    # Merging sum_weekly_data and mean_weekly_data
    twitter_wkly_data = sum_weekly_data.join(mean_weekly_data, 'week')

    return twitter_wkly_data

#Tiktok Cleaner

def TikTokClean(file):
    # Reading in the data into a Spark DataFrame
    df = spark.read.csv(file, header=True)

    # Selecting required columns and renaming them
    df = df.select(
        F.col('commentCount'),
        F.col('createTimeISO').alias('datePosted'),
        F.col('playCount'),
        F.col('shareCount'),
        F.col('diggCount').alias('likeCount')
    )

    # Converting 'datePosted' to a date format
    df = df.withColumn('datePosted', F.to_date(F.unix_timestamp(df['datePosted'].substr(1, 19), "yyyy-MM-dd'T'HH:mm:ss").cast('timestamp')))

    # Calculating week_start and week_end based on datePosted
    week_start = F.expr("date_add(next_day(date_add(datePosted, -1), 'SU'), -6)")
    week_end = F.expr("date_add(next_day(date_add(datePosted, -1), 'SU'), 0)")

    df = df.withColumn('week_start', week_start.cast(DateType())) \
           .withColumn('week_end', week_end.cast(DateType()))
    
    # Add 'week' column using the 'week_start' column
    df = df.withColumn('week', df['week_start'])

    # Assigning week numbers within each week_start and week_end partition
    window = Window.partitionBy('week_start', 'week_end').orderBy('datePosted')
    df = df.withColumn('week', F.row_number().over(window))

    # Aggregating sum and mean values for each week
    weekly_agg = df.groupBy('week_start', 'week_end').agg(
        F.sum('likeCount').alias('ttLikeCountSum'),
        F.sum('playCount').alias('ttPlayCountSum'),
        F.sum('shareCount').alias('ttShareCountSum'),
        F.sum('commentCount').alias('ttCommentCountSum'),
        F.mean('likeCount').alias('ttLikeCountAvg'),
        F.mean('playCount').alias('ttPlayCountAvg'),
        F.mean('shareCount').alias('ttShareCountAvg'),
        F.mean('commentCount').alias('ttCommentCountAvg')
    )
    # Renaming the 'week_start' column to 'week'
    weekly_agg = weekly_agg.withColumnRenamed('week_start', 'week')
    return weekly_agg

#Function for saving the produced data to csv file
def save_to_csv(df, output_file):
    df.write.csv(output_file, mode="overwrite", header=True)

#Function for merging nintindo files by week column
def merge_nintendo_dataframes(df1, df2, df3, on_columns):
    # Drop duplicate columns
    df2 = df2.drop('week_end', 'week_start')
    df3 = df3.drop('week_end', 'week_start')

    # Convert 'week' columns to date type
    df1 = df1.withColumn('week', to_date(df1['week'], "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
    df2 = df2.withColumn('week', to_date(df2['week'], "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
    df3 = df3.withColumn('week', to_date(df3['week'], "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))

    merged_df = df1.join(df2, on_columns).join(df3, on_columns)

    # Select the desired columns in the specified order
    merged_df = merged_df.select(
        'week',
        'twitterViewCountSum',
        'twitterReplyCountSum',
        'twitterRetweetCountSum',
        'twitterLikeCountSum',
        'twitterQuoteCountSum',
        'twitterViewCountMean',
        'twitterReplyCountMean',
        'twitterRetweetCountMean',
        'twitterLikeCountMean',
        'twitterQuoteCountMean',
        'instaLkeCountSum',
        'instaCommentCountSum',
        'instaLikeCountAvg',
        'instaCommentCountAvg',
        'highChange (above 6% change)',
        'aboveAvgVol'
    )

    # Sort the merged DataFrame by 'week' column
    merged_df = merged_df.sort(col('week'))

    return merged_df

#Function for merging gopro files by week column
def merge_gopro_dataframes(df1, df2, df3, df4, on_columns):
    # Drop duplicate columns
    df2 = df2.drop('week_end', 'week_start')
    df3 = df3.drop('week_end', 'week_start')

    # Convert 'week' columns to date type
    df1 = df1.withColumn('week', to_date(df1['week'], "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
    df2 = df2.withColumn('week', to_date(df2['week'], "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
    df3 = df3.withColumn('week', to_date(df3['week'], "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
    df4 = df4.withColumn('week', to_date(df4['week'], "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))

    merged_df = df1.join(df2, on_columns).join(df3, on_columns).join(df4, on_columns)

    # Select the desired columns in the specified order
    merged_df = merged_df.select(
        'week',
        'twitterViewCountSum',
        'twitterReplyCountSum',
        'twitterRetweetCountSum',
        'twitterLikeCountSum',
        'twitterQuoteCountSum',
        'twitterViewCountMean',
        'twitterReplyCountMean',
        'twitterRetweetCountMean',
        'twitterLikeCountMean',
        'twitterQuoteCountMean',
        'instaLkeCountSum',
        'instaCommentCountSum',
        'instaLikeCountAvg',
        'instaCommentCountAvg',
        'ttLikeCountSum',
        'ttPlayCountSum',
        'ttShareCountSum',
        'ttCommentCountSum',
        'ttLikeCountAvg',
        'ttPlayCountAvg',
        'ttShareCountAvg',
        'ttCommentCountAvg',
        'highChange (above 6% change)',
        'aboveAvgVol'
    )

    # Sort the merged DataFrame by 'week' column
    merged_df = merged_df.sort(col('week'))

    return merged_df

#Function for merging nintindo files by week column
def merge_tesla_dataframes(df1, df2, df3, on_columns):
    # Drop duplicate columns
    df2 = df2.drop('week_end', 'week_start')
    df3 = df3.drop('week_end', 'week_start')

    # Convert 'week' columns to date type
    df1 = df1.withColumn('week', to_date(df1['week'], "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
    df2 = df2.withColumn('week', to_date(df2['week'], "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
    df3 = df3.withColumn('week', to_date(df3['week'], "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))

    merged_df = df1.join(df2, on_columns).join(df3, on_columns)

    # Select the desired columns in the specified order
    merged_df = merged_df.select(
        "week",
        "twitterViewCountSum",
        "twitterReplyCountSum",
        "twitterRetweetCountSum",
        "twitterLikeCountSum",
        "twitterQuoteCountSum",
        "twitterViewCountMean",
        "twitterReplyCountMean",
        "twitterRetweetCountMean",
        "twitterLikeCountMean",
        "twitterQuoteCountMean",
        "instaLkeCountSum",
        "instaCommentCountSum",
        "instaLikeCountAvg",
        "instaCommentCountAvg",
        "highChange (above 6% change)",
        "aboveAvgVol"
    )

    # Sort the merged DataFrame by 'week' column
    merged_df = merged_df.sort(col('week'))

    return merged_df

def main():
    #Nintendo Final Data

    nintendo_cleaned_insta = CleanInstagram('Instagram_loaded_data/nintendo_instagram_loaded.csv')
    #save_to_csv(NintindoFinal, "nintendo_cleaned_insta_unmerged.csv")

    nintendo_cleaned_stock = cleanStock('Stock_Data_Per_Company/nintendo_stock.csv')
    #save_to_csv(nintendo_cleaned_stock, 'nintendo_cleaned_stock_unmerged.csv')

    nintendo_twitter_df = TwitterClean('Raw_Data_Twitter_Per_Company/Twitter_Nintendo.csv')
    #save_to_csv(nintendo_twitter_df, 'nintendo_cleaned_twitter_unmerged.csv')

    #Merging the Nintendo data
    nintendo_merged_df = merge_nintendo_dataframes(nintendo_cleaned_insta, nintendo_cleaned_stock, nintendo_twitter_df, ['week'])
    save_to_csv(nintendo_merged_df, 'nintendo_merged.csv')

    #goProFinal Data

    gopro_cleaned_insta = CleanInstagram('Instagram_loaded_data/go_pro_instagram_loaded.csv')
    #save_to_csv(gopro_cleaned_insta, "gopro_cleaned_insta_unmerged.csv")

    gopro_cleaned_stock = cleanStock('Stock_Data_Per_Company/gopro_stock.csv')
    #save_to_csv(gopro_cleaned_stock, 'gopro_cleaned_stock_unmerged.csv')

    gopro_cleaned_twitter = TwitterClean('Raw_Data_Twitter_Per_Company/Twitter_GoPro.csv')
    #save_to_csv(gopro_cleaned_twitter, 'gopro_cleaned_twitter_unmerged.csv')


    gopro_cleaned_tiktok = TikTokClean('Raw_Data_goPro_TikTok/GoPro_TikTok_Data.csv')
    #save_to_csv(gopro_cleaned_tiktok, 'gopro_cleaned_tiktok_unmerged.csv')

    #Merge gopro files and output it to csv
    gopro_merged_df = merge_gopro_dataframes(gopro_cleaned_insta, gopro_cleaned_stock, gopro_cleaned_twitter, gopro_cleaned_tiktok, ['week'])
    save_to_csv(gopro_merged_df, 'gopro_merged.csv')

    #Tesla Final Data

    tesla_cleaned_insta = CleanInstagram('Instagram_loaded_data/tesla_instagram_loaded.csv')
    #save_to_csv(teslaFinal, "tesla_cleaned_insta_unmerged.csv")

    tesla_cleaned_stock = cleanStock('Stock_Data_Per_Company/tesla_stock.csv')
    #save_to_csv(tesla_cleaned_stock, 'tesla_cleaned_stock_unmerged.csv')

    tesla_cleaned_twitter = TwitterClean('Raw_Data_Twitter_Per_Company/Twitter_TeslaElon.csv')
    #save_to_csv(tesla_twitter_df, 'tesla_cleaned_twitter_unmerged.csv')

    #Merge tesla files and output it to csv
    tesla_merged_df = merge_tesla_dataframes(tesla_cleaned_insta, tesla_cleaned_stock, tesla_cleaned_twitter, ['week'])
    save_to_csv(tesla_merged_df, 'tesla_merged.csv')

if __name__ == '__main__':
    main()

spark.stop()

end_time = time.time()
duration = end_time - start_time
print("Execution time: {:.2f} seconds".format(duration))
