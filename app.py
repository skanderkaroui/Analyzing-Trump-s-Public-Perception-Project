import sqlite3
import pandas as pd

# Constants
REDDIT_FILE = 'reddit_trump.txt'
TWEETS_FILE = 'trump_tweets.csv'
DATABASE = 'social_analysis.db'

def define_organization():
    """Define the purpose of the project."""
    print("This project is aimed at analyzing social media data related to Donald Trump.")
    print("We will work with datasets of Reddit comments and Trump's tweets.")

def extract_data(reddit_file, tweets_file):
    """Extract data from Reddit and Twitter datasets."""
    print("Extracting data from files...")
    reddit_data = pd.read_csv(reddit_file, delimiter='|', on_bad_lines='skip')
    tweets_data = pd.read_csv(tweets_file)
    print(f"Reddit data: {reddit_data.shape[0]} rows loaded.")
    print(f"Tweets data: {tweets_data.shape[0]} rows loaded.")
    return reddit_data, tweets_data

def define_database_model(cursor):
    """Define the database schema."""
    print("Defining database schema...")
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
    """Load data into the database."""
    print("Loading data into the database...")
    tweets_data.to_sql('trump_tweets', conn, if_exists='replace', index=False)
    reddit_data.to_sql('reddit_comments', conn, if_exists='replace', index=False)
    print("Data successfully loaded into the database.")

def execute_queries(conn):
    """Execute predefined queries on the data."""
    print("Executing queries on the data...")
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
        "Average Favorites per Tweet": '''
            SELECT AVG(favorites) AS average_favorites
            FROM trump_tweets;
        ''',
        "Top 5 Most Commented Reddit Posts": '''
            SELECT title, comments_count
            FROM reddit_comments
            WHERE comments_count IS NOT NULL
            ORDER BY comments_count DESC
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

def main():
    """Main function to run the program."""
    define_organization()
    reddit_data, tweets_data = extract_data(REDDIT_FILE, TWEETS_FILE)

    # Connect to database and define schema
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    define_database_model(cursor)

    # Load data into the database
    load_data_to_db(conn, reddit_data, tweets_data)

    # Execute queries
    execute_queries(conn)

    # Close connection
    conn.close()
    print("Program completed.")

if __name__ == '__main__':
    main()