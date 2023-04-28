#!/usr/bin/env python3

import sys
import json
import pandas as pd

def mapper():
    df = pd.read_csv(sys.stdin)

    # Select only the columns that we need
    df = df[['ownerUsername', 'timestamp', 'likesCount', 'commentsCount']]

    # Calculate the average likesCount and commentsCount
    avg_likes = df['likesCount'].mean()
    avg_comments = df['commentsCount'].mean()

    # Calculate the deviation of likesCount and commentsCount from the average
    df['percent_likes_dev'] = ((df['likesCount'] - avg_likes) / avg_likes) * 100
    df['percent_comments_dev'] = ((df['commentsCount'] - avg_comments) / avg_comments) * 100

    # Create a new DataFrame with only posts that have like and comment counts over the average
    df_over_avg = df[(df['likesCount'] > avg_likes) & (df['commentsCount'] > avg_comments)].copy()

    df_over_avg['Date'] = pd.to_datetime(df_over_avg['timestamp']).dt.strftime('%Y-%m-%d')

    # Output the data in JSON format to be used by the reducer
    for index, row in df_over_avg.iterrows():
        print(json.dumps(row.to_dict()))

if __name__ == "__main__":
    mapper()
