import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from textblob import TextBlob
from wordcloud import WordCloud
import numpy as np
from datetime import datetime
import re  # Import the regular expressions module

class SocialMediaAnalyzer:
    def __init__(self, conn):
        self.conn = conn

    def perform_sentiment_analysis(self):
        """Analyze sentiment of tweets and Reddit comments"""
        tweets_df = pd.read_sql_query("SELECT text, datetime FROM trump_tweets", self.conn)
        tweets_df['sentiment'] = tweets_df['text'].apply(lambda x: TextBlob(str(x)).sentiment.polarity)
        
        # Aggregate sentiment by date
        tweets_df['date'] = pd.to_datetime(tweets_df['datetime']).dt.date
        daily_sentiment = tweets_df.groupby('date')['sentiment'].mean()
        
        # Plot sentiment over time
        plt.figure(figsize=(12, 6))
        daily_sentiment.plot(kind='line')
        plt.title('Tweet Sentiment Over Time')
        plt.xlabel('Date')
        plt.ylabel('Sentiment Score')
        plt.savefig('sentiment_analysis.png')
        plt.close()
        
        return daily_sentiment.describe()

    def generate_engagement_metrics(self):
        """Analyze engagement patterns"""
        query = """
        SELECT 
            date,
            AVG(favorites) as avg_favorites,
            AVG(retweets) as avg_retweets,
            COUNT(*) as tweet_count
        FROM trump_tweets
        GROUP BY date
        ORDER BY date
        """
        engagement_df = pd.read_sql_query(query, self.conn)
        
        # Calculate engagement ratio
        engagement_df['engagement_ratio'] = (
            engagement_df['avg_favorites'] + engagement_df['avg_retweets']
        ) / engagement_df['tweet_count']
        
        return engagement_df

    def create_word_cloud(self, source='tweets'):
        """Generate word cloud from tweets or Reddit comments"""
        if source == 'tweets':
            text_data = pd.read_sql_query("SELECT text FROM trump_tweets", self.conn)['text']
        else:
            try:
                # First, let's check the actual column names
                columns_query = "SELECT * FROM reddit_comments LIMIT 1"
                columns_df = pd.read_sql_query(columns_query, self.conn)
                print("Available columns:", columns_df.columns.tolist())
                
                # Use 'Comments' instead of 'comments' (note the capital C)
                text_data = pd.read_sql_query("SELECT Comments FROM reddit_comments", self.conn)['Comments']
            except Exception as e:
                print(f"Error accessing Reddit comments: {str(e)}")
                raise
        
        # Clean the text data by removing URLs
        text_data = text_data.apply(lambda x: re.sub(r'http\S+|www\S+|https\S+', '', str(x), flags=re.MULTILINE))

        all_text = ' '.join(str(text) for text in text_data)
        wordcloud = WordCloud(width=800, height=400, background_color='white').generate(all_text)
        
        plt.figure(figsize=(10, 5))
        plt.imshow(wordcloud, interpolation='bilinear')
        plt.axis('off')
        plt.savefig(f'{source}_wordcloud.png')
        plt.close()

    def analyze_posting_patterns(self):
        """Analyze posting time patterns"""
        tweets_df = pd.read_sql_query(
            "SELECT datetime, device FROM trump_tweets", 
            self.conn
        )
        tweets_df['datetime'] = pd.to_datetime(tweets_df['datetime'])
        tweets_df['hour'] = tweets_df['datetime'].dt.hour
        
        hourly_device_dist = pd.crosstab(tweets_df['hour'], tweets_df['device'])
        
        plt.figure(figsize=(16, 8))
        hourly_device_dist.plot(kind='bar', stacked=True)
        plt.title('Tweet Distribution by Hour and Device')
        plt.xlabel('Hour of Day')
        plt.ylabel('Number of Tweets')
        
        plt.close()
        
        return hourly_device_dist 