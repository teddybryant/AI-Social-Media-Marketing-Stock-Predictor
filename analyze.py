import json
import pandas as pd
import matplotlib.pyplot as plt


def analyze(file1,file2):

    df = pd.read_csv(file1)

    # Select only the columns that we need
    df = df[['ownerUsername', 'timestamp', 'likesCount', 'commentsCount']]

    # Rename the columns to match the original names
    df = df.rename(columns={
        'ownerUsername': 'ownerUsername',
        'timestamp': 'timestamp',
        'likesCount': 'likesCount',
        'commentsCount': 'commentsCount'
    })

    # Calculate the average likesCount and commentsCount
    avg_likes = df['likesCount'].mean()
    avg_comments = df['commentsCount'].mean()

    # Calculate the deviation of likesCount and commentsCount from the average
    df['percent_likes_dev'] = ((df['likesCount'] - avg_likes)/avg_likes)*100
    df['percent_comments_dev'] = ((df['commentsCount'] - avg_comments)/avg_comments)*100

        # Count the number of posts with both comment and likes counts over the average
    count = df[(df['likesCount'] > avg_likes) & (df['commentsCount'] > avg_comments)].shape[0]

    # Create a new DataFrame with only posts that have like and comment counts over the average
    df_over_avg = df[(df['likesCount'] > avg_likes) & (df['commentsCount'] > avg_comments)]

    #df_over_avg.to_csv('cleaned_nintendo_over_avg_insta.csv', index=False)

    # Print the DataFrame
    print("Average Comment Count: " + str(avg_comments))
    print("Average Like Count: " + str(avg_likes))
    #print("Number of Posts over average: " + str(count))
    #print(df)
    #print(df_over_avg)


    # read in the CSV file as a DataFrame
    df2 = pd.read_csv(file2)

    avg_volume = df2['Volume'].mean()

    # create a new DataFrame with the date, average price, and volume columns
    stock_df = pd.DataFrame({'Date': df2['Date'],
                                'Volume': df2['Volume'],
                        'Change_Price': abs(((df2['Close'] - df2['Open']) / df2['Open'])*10000),
                        'Change_Volume': abs(((df2['Volume'] - avg_volume) / avg_volume)*100)})


    avg_change_price = abs(stock_df['Change_Price']).mean() 
    avg_change_volume = abs(stock_df['Change_Volume']).mean()

    print("AVERAGE VOLUME: " + str(avg_volume)) 

    print("AVERAGE PERCENT CHANGE IN PRICE: " + str(avg_change_price))
    print("AVERAGE PERCENT CHANGE IN VOLUME: " + str(avg_change_volume))

    # print the new DataFrame
    #print(stock_df)
    df = df.rename(columns={'timestamp': 'Date'})
    df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')

    merged_df = pd.merge(df, stock_df, on='Date')

    #merged_df["AvgPriceStdDev"] = merged_df.apply(lambda row: abs(row["Average"] - row["Close"]) / row["Close"], axis=1)

    print(merged_df)
    #merged_df.to_csv('test.csv', index=False)


    # create a line plot of the data
    #merged_df.plot(x='Date', y=['percent_likes_dev','percent_comments_dev','Change_Price','Change_Volume'])
    #plt.show()

  

#RUNS FOR ALL THREE COMPANIES

analyze('nintendo_instagram.csv','nintendo_stock.csv')
analyze('gopro_instagram.csv','gopro_stock.csv')
analyze('tesla_instagram.csv','tesla_stock.csv')