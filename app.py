import sqlite3
import pandas as pd

# Step 1: Define the organization for the work
def define_organization():
    print("This project is aimed at analyzing social media data related to Donald Trump.")
    print("We will work with datasets of Reddit comments and Trump's tweets.")

# Step 2: Extract data from social media datasets
def extract_data(reddit_file, tweets_file):
    print("Extracting data from files...")
    
    # Load Reddit data
    reddit_data = pd.read_csv(reddit_file, delimiter='|', on_bad_lines='skip')
    
    # Load Tweets data
    tweets_data = pd.read_csv(tweets_file)
    
    print(f"Reddit data: {reddit_data.shape[0]} rows loaded.")
    print(f"Tweets data: {tweets_data.shape[0]} rows loaded.")
    
    return reddit_data, tweets_data

# Step 3: Define the database model
def define_database_model(cursor):
    print("Defining database schema...")
    
    # Create table for tweets
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
    
    # Create table for Reddit comments
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

# Step 4: Load data into the database
def load_data_to_db(conn, reddit_data, tweets_data):
    print("Loading data into the database...")
    
    # Load tweets data
    tweets_data.to_sql('trump_tweets', conn, if_exists='replace', index=False)
    
    # Load Reddit comments data
    reddit_data.to_sql('reddit_comments', conn, if_exists='replace', index=False)
    
    print("Data successfully loaded into the database.")

# Step 5: Execute queries on the data
def execute_queries(conn):
    print("Executing queries on the data...")
    
    # Example Query 1: Top 5 most retweeted tweets
    query1 = '''
        SELECT text, retweets
        FROM trump_tweets
        ORDER BY retweets DESC
        LIMIT 5;
    '''
    top_retweets = pd.read_sql_query(query1, conn)
    print("Top 5 most retweeted tweets:")
    print(top_retweets)
    
    # Example Query 2: Top Reddit posts by points
    query2 = '''
        SELECT title, points
        FROM reddit_comments
        WHERE points IS NOT NULL
        ORDER BY points DESC
        LIMIT 5;
    '''
    top_reddit_posts = pd.read_sql_query(query2, conn)
    print("Top 5 Reddit posts by points:")
    print(top_reddit_posts)

# Main program
def main():
    reddit_file = 'reddit_trump.txt'
    tweets_file = 'trump_tweets.csv'
    database = 'social_analysis.db'
    
    # Step 1: Define the organization
    define_organization()
    
    # Step 2: Extract data
    reddit_data, tweets_data = extract_data(reddit_file, tweets_file)
    
    # Step 3: Connect to database and define schema
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    define_database_model(cursor)
    
    # Step 4: Load data into the database
    load_data_to_db(conn, reddit_data, tweets_data)
    
    # Step 5: Execute queries
    execute_queries(conn)
    
    # Close connection
    conn.close()
    print("Program completed.")

if __name__ == '__main__':
    main()