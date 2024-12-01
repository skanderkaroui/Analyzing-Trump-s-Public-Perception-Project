import sqlite3
import pandas as pd
from analysis import SocialMediaAnalyzer
import logging
from datetime import datetime

# Constants
REDDIT_FILE = 'reddit_trump.txt'
TWEETS_FILE = 'trump_tweets.csv'
DATABASE = 'social_analysis.db'

# Add logging configuration
logging.basicConfig(
    filename=f'social_analysis_{datetime.now().strftime("%Y%m%d")}.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def define_organization():
    print("This project is aimed at analyzing social media data related to Donald Trump.")
    print("We will work with datasets of Reddit comments and Trump's tweets.")

def extract_data(reddit_file, tweets_file):
    reddit_data = pd.read_csv(reddit_file, delimiter='|', on_bad_lines='skip')
    tweets_data = pd.read_csv(tweets_file)
    print(f"Reddit data: {reddit_data.shape[0]} rows loaded.")
    print(f"Tweets data: {tweets_data.shape[0]} rows loaded.")
    return reddit_data, tweets_data

def define_database_model(cursor):
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS trump_tweets (
            id INTEGER PRIMARY KEY,
            text TEXT,
            is_retweet BOOLEAN,
            is_deleted BOOLEAN,
            device TEXT,
            favorites INTEGER,
            retweets INTEGER,
            datetime TEXT,
            is_flagged BOOLEAN,
            date TEXT
        );
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS reddit_comments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            points INTEGER,
            composite_score TEXT,
            post TEXT,
            comments_lang TEXT,
            post_location TEXT,
            meta TEXT,
            submission_location TEXT,
            title TEXT,
            latlon TEXT,
            comments TEXT,
            comments_count INTEGER,
            post_date TEXT
        );
    ''')
    print("Database schema defined.")

def load_data_to_db(conn, reddit_data, tweets_data):
    tweets_data.to_sql('trump_tweets', conn, if_exists='replace', index=False)
    reddit_data.to_sql('reddit_comments', conn, if_exists='replace', index=False)
    print("Data successfully loaded into the database.")

def execute_queries(conn):
    queries = {
        "Top 5 most retweeted tweets": '''
            SELECT text, retweets
            FROM trump_tweets
            ORDER BY retweets DESC
            LIMIT 5;
        ''',
        "Top Reddit posts by points": '''
            SELECT title, points
            FROM reddit_comments
            WHERE points IS NOT NULL
            ORDER BY points DESC
            LIMIT 5;
        ''',
        "Count of Tweets by Device": '''
            SELECT device, COUNT(*) AS tweet_count
            FROM trump_tweets
            GROUP BY device;
        ''',
        "Average Retweets per Tweet": '''
            SELECT AVG(retweets) AS average_retweets
            FROM trump_tweets;
        ''',
        "Top 5 Reddit Posts by Submission Location": '''
            SELECT title, submission_location, COUNT(*) AS post_count
            FROM reddit_comments
            GROUP BY title, submission_location
            ORDER BY post_count DESC
            LIMIT 5;
        ''',
        "Count of Retweets": '''
            SELECT COUNT(*) AS retweet_count
            FROM trump_tweets
            WHERE is_retweet = 'TRUE';
        '''
    }

    for description, query in queries.items():
        result = pd.read_sql_query(query, conn)
        print(f"{description}:")
        print(result)

def perform_advanced_analysis(conn):
    analyzer = SocialMediaAnalyzer(conn)
    
    logging.info("Starting advanced analysis...")
    
    # Perform sentiment analysis
    sentiment_stats = analyzer.perform_sentiment_analysis()
    logging.info("Sentiment analysis completed")
    print("\nSentiment Analysis Statistics:")
    print(sentiment_stats)
    
    # Generate engagement metrics
    engagement_df = analyzer.generate_engagement_metrics()
    logging.info("Engagement analysis completed")
    print("\nEngagement Metrics Summary:")
    print(engagement_df.describe())
    
    # Create word clouds
    # analyzer.create_word_cloud('reddit')
    
    # Analyze posting patterns
    posting_patterns = analyzer.analyze_posting_patterns()
    logging.info("Posting patterns analysis completed")
    print("\nPosting Patterns by Hour:")
    print(posting_patterns.head())

def main():
    try:
        logging.info("Starting social media analysis program")
        define_organization()
        reddit_data, tweets_data = extract_data(REDDIT_FILE, TWEETS_FILE)

        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()
        define_database_model(cursor)

        load_data_to_db(conn, reddit_data, tweets_data)
        execute_queries(conn)
        
        # Perform advanced analysis
        perform_advanced_analysis(conn)

        conn.close()
        logging.info("Program completed successfully")
        print("Program completed. Check the generated visualizations and log file for details.")
        
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        raise

if __name__ == '__main__':
    main()