from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, weekofyear, year, to_date, sum as _sum, mean as _mean, concat_ws, when, first, last, min, max

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

    sum_data = df.groupBy("week").agg(
        _sum("likesCount").alias("instaLikeCountSum"),
        _sum("commentsCount").alias("instaCommentCountSum")
    )

    mean_data = df.groupBy("week").agg(
        _mean("likesCount").alias("instaLikeCountAvg"),
        _mean("commentsCount").alias("instaCommentCountAvg")
    )

    insta_weekly_data = sum_data.join(mean_data, on="week")

    return insta_weekly_data

def clean_stock(file):
    stock = spark.read.csv(file, header=True, inferSchema=True)
    stock = stock.select("Date", "Close", "Volume")
    stock = stock.withColumn("Date", to_date(col("Date"), "yyyy-MM-dd"))
    stock = stock.withColumn("week", weekofyear(col("Date")))
    stock = stock.withColumn("year", year(col("Date")))

    sum_volume = stock.groupBy("week").agg(
        _sum("Volume").alias("weeklyVol")
    )

    avg_vol = sum_volume.select(_mean("weeklyVol").alias("averageVol")).collect()[0]["averageVol"]
    sum_volume = sum_volume.withColumn("aboveAvgVol", when(col("weeklyVol") > avg_vol, 1).otherwise(0)).drop("weeklyVol")

    stock = stock.join(sum_volume, on="week")
    stock = stock.groupBy("week").agg(
        min("Date").alias("startDate"),
        max("Date").alias("endDate"),
        first("Close").alias("Close"),
        first("aboveAvgVol").alias("aboveAvgVol")
    )

    return stock


def clean_twitter(file):
    df = spark.read.csv(file, header=True, inferSchema=True)
    df = df.select("created_at", "retweet_count", "favorite_count")
    df = df.withColumnRenamed("created_at", "datePosted")

    df = df.withColumn("datePosted", to_date(col("datePosted"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
    df = df.orderBy("datePosted")
    df = df.withColumn("week", weekofyear(col("datePosted")))
    df = df.withColumn("year", year(col("datePosted")))

    sum_data = df.groupBy("week").agg(
        _sum("retweet_count").alias("retweetCountSum"),
        _sum("favorite_count").alias("favoriteCountSum")
    )

    mean_data = df.groupBy("week").agg(
        _mean("retweet_count").alias("retweetCountAvg"),
        _mean("favorite_count").alias("favoriteCountAvg")
    )

    twitter_weekly_data = sum_data.join(mean_data, on="week")

    return twitter_weekly_data

def clean_tiktok(file):
    df = spark.read.csv(file, header=True, inferSchema=True)
    df = df.select("create_time", "diggCount", "commentCount")
    df = df.withColumnRenamed("create_time", "datePosted")
    df = df.withColumn("datePosted", to_date(col("datePosted"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
    df = df.orderBy("datePosted")
    df = df.withColumn("week", weekofyear(col("datePosted")))
    df = df.withColumn("year", year(col("datePosted")))

    sum_data = df.groupBy("week").agg(
        _sum("diggCount").alias("tiktokLikeCountSum"),
        _sum("commentCount").alias("tiktokCommentCountSum")
    )

    mean_data = df.groupBy("week").agg(
        _mean("diggCount").alias("tiktokLikeCountAvg"),
        _mean("commentCount").alias("tiktokCommentCountAvg")
    )

    tiktok_weekly_data = sum_data.join(mean_data, on="week")

    return tiktok_weekly_data


def save_to_csv(df: DataFrame, file_path: str):
    df.write.csv(file_path, mode="overwrite", header=True)


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
    gopro_final_df.show()
    save_to_csv(gopro_final_df, "gopro_final_data.csv")

    end_time = time.time()
    print("Execution time: {:.2f} seconds".format(end_time - start_time))
