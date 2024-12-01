import pandas as pd
import matplotlib.pyplot as plt
from textblob import TextBlob
import sqlite3
import logging

DATABASE = 'social_analysis.db'

def perform_reddit_sentiment_analysis(conn):
    """Analyze sentiment of Reddit comments"""
    try:
        # Get column names from the table
        columns_query = "SELECT * FROM reddit_comments LIMIT 1"
        columns_df = pd.read_sql_query(columns_query, conn)
        print("Available columns in reddit_comments table:", columns_df.columns.tolist())
        
        # Query the data - using 'comments' column
        reddit_df = pd.read_sql_query(
            "SELECT comments, post_date FROM reddit_comments WHERE comments IS NOT NULL", 
            conn
        )
        
        # Calculate sentiment for comments
        reddit_df['sentiment'] = reddit_df['comments'].apply(
            lambda x: TextBlob(str(x)).sentiment.polarity if pd.notnull(x) else 0
        )
        
        # Convert post_date to datetime
        reddit_df['date'] = pd.to_datetime(reddit_df['post_date'], errors='coerce').dt.date
        
        # Remove any null dates
        reddit_df = reddit_df.dropna(subset=['date'])
        
        # Aggregate sentiment by date
        daily_reddit_sentiment = reddit_df.groupby('date')['sentiment'].mean()
        
        print(f"Processed {len(reddit_df)} Reddit comments")
        return daily_reddit_sentiment
        
    except Exception as e:
        print(f"Error in Reddit sentiment analysis: {str(e)}")
        raise



def main():
    print("Starting sentiment analysis...")
    conn = sqlite3.connect(DATABASE)
    print("Connected to the database.")
    
    try:
        # Perform sentiment analysis on tweets
        print("Analyzing tweet sentiment...")
        tweets_df = pd.read_sql_query(
            "SELECT text, datetime FROM trump_tweets WHERE text IS NOT NULL", 
            conn
        )
        tweets_df['sentiment'] = tweets_df['text'].apply(
            lambda x: TextBlob(str(x)).sentiment.polarity if pd.notnull(x) else 0
        )
        tweets_df['date'] = pd.to_datetime(tweets_df['datetime']).dt.date
        daily_tweet_sentiment = tweets_df.groupby('date')['sentiment'].mean()
        
        print(f"Processed {len(tweets_df)} tweets")
        
        # Perform sentiment analysis on Reddit posts
        print("Analyzing Reddit sentiment...")
        daily_reddit_sentiment = perform_reddit_sentiment_analysis(conn)
        
        
        print("Sentiment analysis completed successfully.")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        raise
    finally:
        conn.close()
        print("Database connection closed.")

if __name__ == '__main__':
    main()