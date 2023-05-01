from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, weekofyear, year, to_date, sum as _sum, mean as _mean, concat_ws, when, first, last, min, max, avg
from pyspark.sql.window import Window
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Social Media and Stock Data Analysis") \
    .getOrCreate()

def clean_instagram(instagram_file):
    df = spark.read.csv(instagram_file, header=True, inferSchema=True)
    df = df.select( 
        "timestamp",
        "likesCount",
        "commentsCount" 
    ).withColumnRenamed("timestamp", "datePosted")

    df = df.withColumn("datePosted", to_date(col("datePosted"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
    df = df.orderBy("datePosted")
    df = df.withColumn("week", weekofyear(col("datePosted")))
    df = df.withColumn("year", year(col("datePosted")))

    sum_data = df.groupBy("week", "year").agg(
        _sum("likesCount").alias("instaLikeCountSum"),
        _sum("commentsCount").alias("instaCommentCountSum")
    )

    mean_data = df.groupBy("week", "year").agg(
        _mean("likesCount").alias("instaLikeCountAvg"),
        _mean("commentsCount").alias("instaCommentCountAvg")
    )

    insta_weekly_data = sum_data.join(mean_data, on=["week", "year"])
    return insta_weekly_data

def clean_stock(stock_file):
    stock = spark.read.csv(stock_file, header=True, inferSchema=True)

    stock = stock.withColumn("Date", to_date(col("Date"), "yyyy-MM-dd"))
    stock = stock.withColumn("week", weekofyear(col("Date")))
    stock = stock.withColumn("year", year(col("Date")))

    window = Window.partitionBy("week", "year").orderBy("Date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

    stock = stock.withColumn("startDate", first("Date").over(window))
    stock = stock.withColumn("endDate", last("Date").over(window))
    stock = stock.groupBy("week", "year", "startDate", "endDate").agg(first("Close").alias("startClose"), last("Close").alias("endClose"))

    stock = stock.withColumn("week_range", F.concat(F.col("startDate"), F.lit("/"), F.col("endDate")))

    stock = stock.select("week", "year", "week_range", "startClose", "endClose")

    return stock





def clean_twitter(file):
    df = spark.read.csv(file, header=True, inferSchema=True)
    df = df.select("created_at", "retweet_count", "favorite_count", "quote_count", "reply_count", "view_count")
    df = df.withColumnRenamed("created_at", "datePosted")

    df = df.withColumn("datePosted", to_date(col("datePosted"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
    df = df.orderBy("datePosted")
    df = df.withColumn("week", weekofyear(col("datePosted")))
    df = df.withColumn("year", year(col("datePosted")))

    sum_data = df.groupBy("week", "year").agg(
        _sum("retweet_count").alias("twitterRetweetCountSum"),
        _sum("favorite_count").alias("twitterLikeCountSum"),
        _sum("quote_count").alias("twitterQuoteCountSum"),
        _sum("reply_count").alias("twitterReplyCountSum"),
        _sum("view_count").alias("twitterViewCountSum")
    )

    mean_data = df.groupBy("week", "year").agg(
        _mean("retweet_count").alias("twitterRetweetCountMean"),
        _mean("favorite_count").alias("twitterLikeCountMean"),
        _mean("quote_count").alias("twitterQuoteCountMean"),
        _mean("reply_count").alias("twitterReplyCountMean"),
        _mean("view_count").alias("twitterViewCountMean")
    )

    twitter_weekly_data = sum_data.join(mean_data, on=["week", "year"])
    return twitter_weekly_data

def clean_tiktok(file):
    df = spark.read.csv(file, header=True, inferSchema=True)
    df = df.select("createTime", "diggCount", "commentCount", "playCount", "shareCount")
    df = df.withColumnRenamed("createTime", "datePosted")
    df = df.withColumn("datePosted", to_date(col("datePosted"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
    df = df.orderBy("datePosted")
    df = df.withColumn("week", weekofyear(col("datePosted")))
    df = df.withColumn("year", year(col("datePosted")))

    sum_data = df.groupBy("week", "year").agg(
        _sum("diggCount").alias("ttLikeCountSum"),
        _sum("commentCount").alias("ttCommentCountSum"),
        _sum("playCount").alias("ttPlayCountSum"),
        _sum("shareCount").alias("ttShareCountSum")
    )

    mean_data = df.groupBy("week", "year").agg(
        _mean("diggCount").alias("ttLikeCountAvg"),
        _mean("commentCount").alias("ttCommentCountAvg"),
        _mean("playCount").alias("ttPlayCountAvg"),
        _mean("shareCount").alias("ttShareCountAvg")
    )

    tiktok_weekly_data = sum_data.join(mean_data, on=["week", "year"])
    return tiktok_weekly_data


def save_to_csv(df, output_file):
    df.write.csv(output_file, mode="overwrite", header=True)

if __name__ == "__main__":
    import time
    start_time = time.time()

    # Nintendo data
    nintendo_insta_df = clean_instagram("nintendo_instagram.csv")
    nintendo_cleaned_stock = clean_stock("nintendo_stock.csv")
    nintendo_twitter_df = clean_twitter("Twitter_Nintendo.csv")

    nintendo_final_df = (
        nintendo_insta_df.join(nintendo_cleaned_stock, on=["week", "year"])
        .join(nintendo_twitter_df, on=["week", "year"])
        .withColumn("week", concat_ws("/", "startDate", "endDate"))
        .drop("startDate", "endDate")
    )

    nintendo_final_df = nintendo_final_df.select(
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
        "instaLikeCountSum",
        "instaCommentCountSum",
        "instaLikeCountAvg",
        "instaCommentCountAvg",
        "highChange",
        "aboveAvgVol"
    )

    nintendo_final_df.show()
    save_to_csv(nintendo_final_df, "nintendo_final_data.csv")

    # Tesla data
    tesla_insta_df = clean_instagram("tesla_instagram.csv")
    tesla_cleaned_stock = clean_stock("tesla_stock.csv")
    tesla_twitter_df = clean_twitter("Twitter_TeslaElon.csv")

    tesla_final_df = (
        tesla_insta_df.join(tesla_cleaned_stock, on=["week", "year"])
        .join(tesla_twitter_df, on=["week", "year"])
        .withColumn("week", concat_ws("/", "startDate", "endDate"))
        .drop("startDate", "endDate")
    )

    tesla_final_df = tesla_final_df.select(
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
        "instaLikeCountSum",
        "instaCommentCountSum",
        "instaLikeCountAvg",
        "instaCommentCountAvg",
        "highChange",
        "aboveAvgVol"
    )

    tesla_final_df.show()
    save_to_csv(tesla_final_df, "tesla_final_data.csv")

    # GoPro data
    gopro_insta_df = clean_instagram("gopro_instagram.csv")
    gopro_cleaned_stock = clean_stock("gopro_stock.csv")
    gopro_twitter_df = clean_twitter("Twitter_GoPro.csv")
    gopro_tiktok_df = clean_tiktok("GoPro_TikTok_Data.csv")

    gopro_final_df = (
        gopro_insta_df.join(gopro_cleaned_stock, on=["week", "year"])
        .join(gopro_twitter_df, on=["week", "year"])
        .join(gopro_tiktok_df, on=["week", "year"])
        .withColumn("week", concat_ws("/", "startDate", "endDate"))
        .drop("startDate", "endDate")
    )

    gopro_final_df = gopro_final_df.select(
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
        "instaLikeCountSum",
        "instaCommentCountSum",
        "instaLikeCountAvg",
        "instaCommentCountAvg",
        "ttLikeCountSum",
        "ttPlayCountSum",
        "ttShareCountSum",
        "ttCommentCountSum",
        "ttLikeCountAvg",
        "ttPlayCountAvg",
        "ttShareCountAvg",
        "ttCommentCountAvg",
        "highChange",
        "aboveAvgVol"
    )

    gopro_final_df.show()
    save_to_csv(gopro_final_df, "gopro_final_data.csv")

    end_time = time.time()
    print("Execution time: {:.2f} seconds".format(end_time - start_time))