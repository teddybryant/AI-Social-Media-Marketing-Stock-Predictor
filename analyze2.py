import json
import pandas as pd
import matplotlib.pyplot as plt

# Open the JSON file for reading
with open('nintendo_instagram.json', 'r') as json_file:
    # Load the JSON data from the file
    json_data = json.load(json_file)

    # Create an empty list to store the rows of the DataFrame
    rows = []

    # Loop through each JSON object in the file
    for json_obj in json_data:
        # Extract the fields from the JSON object
        owner_username = json_obj.get('ownerUsername')
        time_stamp = json_obj.get('timestamp')
        likes_count = json_obj.get('likesCount')
        comments_count = json_obj.get('commentsCount')

        # Add a new row to the list of rows
        rows.append({
            'ownerUsername': owner_username,
            'timestamp': time_stamp,
            'likesCount': likes_count,
            'commentsCount': comments_count
        })

    # Create a Pandas DataFrame from the list of rows
    df = pd.DataFrame(rows)

    # Calculate the average likesCount and commentsCount
    avg_likes = df['likesCount'].mean()
    avg_comments = df['commentsCount'].mean()

    # Calculate the deviation of likesCount and commentsCount from the average
    df['likes_deviation'] = df['likesCount'] - avg_likes
    df['comments_deviation'] = df['commentsCount'] - avg_comments

     # Count the number of posts with both comment and likes counts over the average
    count = df[(df['likesCount'] > avg_likes) & (df['commentsCount'] > avg_comments)].shape[0]

    # Create a new DataFrame with only posts that have like and comment counts over the average
    df_over_avg = df[(df['likesCount'] > avg_likes) & (df['commentsCount'] > avg_comments)]

    #df_over_avg.to_csv('cleaned_nintendo_over_avg_insta.csv', index=False)

    # Print the DataFrame
    #print("Average Comment Count: " + str(avg_comments))
    #print("Average Like Count: " + str(avg_likes))
    #print("Number of Posts over average: " + str(count))
    #print(df)
    #print(df_over_avg)


    # read in the CSV file as a DataFrame
    df2 = pd.read_csv('nintendo_stock.csv')

    # create a new DataFrame with the date, average price, and volume columns
    stock_df = pd.DataFrame({'Date': df2['Date'],
                        'Average': (df2['High'] - df2['Low']) / 2,
                        'Close': df2['Close'],
                        'Volume': df2['Volume']})

    # print the new DataFrame
    #print(stock_df)
    df = df.rename(columns={'timestamp': 'Date'})
    df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')

    merged_df = pd.merge(df, stock_df, on='Date')

    merged_df["AvgPriceStdDev"] = merged_df.apply(lambda row: abs(row["Average"] - row["Close"]) / row["Close"], axis=1)

    print(merged_df)
    merged_df.to_csv('test.csv', index=False)


# create a line plot of the data
    merged_df.plot(x='Date', y=['AvgPriceStdDev','likes_deviation'])
    plt.show()

    #DOES NOT SHOW MUCH BUT MAYBE CHANGE DEVIATIONS TO PERCENTAGE RATHER THAN NUMBER