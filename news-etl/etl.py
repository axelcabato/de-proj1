import os
import psycopg2
from newsdataapi import NewsDataApiClient

# Read the API key from an environment variable
API_KEY: str | None = os.environ.get("NEWS_API_KEY")

if not API_KEY:
    print("Error: NEWS_API_KEY environment variable not set.")
    exit(1)

# Type checker hint: API_KEY guaranteed to be str
api = NewsDataApiClient(apikey=API_KEY)  # type: ignore


def fetch_and_store_articles():
    """
    Fetches news articles from NewsData.io API and stores them in a PostgreSQL database.
    """
    try:
        response_data = api.news_api(language="en")

    except Exception as e:
        print(f"Failed to fetch data from API: {e}")
        return

    if response_data and response_data.get("status") == "success":
        articles_to_store = response_data.get("results", [])
        if not articles_to_store:
            print("No articles found to store.")
            return

        print(f"Successfully fetched {len(articles_to_store)} articles.")

        try:
            POSTGRES_URL = os.environ.get("POSTGRES_URL")
            if not POSTGRES_URL:
                raise ValueError("POSTGRES_URL not set")
            
            with psycopg2.connect(POSTGRES_URL) as conn:
                with conn.cursor() as cursor:
                    # Create table if it doesn't exist (with new columns)
                    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS articles (
                        id TEXT PRIMARY KEY,
                        title TEXT,
                        author TEXT,
                        body TEXT,
                        source TEXT,
                        published_at TEXT,
                        sentiment_score REAL,
                        word_count INTEGER,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """)

                    # Handle schema evolution: add columns if they don't exist
                    # This allows the pipeline to work with databases created before these columns existed
                    alter_statements = [
                        "ALTER TABLE articles ADD COLUMN IF NOT EXISTS sentiment_score REAL",
                        "ALTER TABLE articles ADD COLUMN IF NOT EXISTS word_count INTEGER",
                        "ALTER TABLE articles ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
                        "ALTER TABLE articles ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
                    ]

                    for stmt in alter_statements:
                        cursor.execute(stmt)

                    # Insert/update all articles
                    inserted_count = 0
                    for item in articles_to_store:
                        creator = item.get("creator")
                        if isinstance(creator, list):
                            creator = ", ".join(creator)

                        article = {
                            "id": item.get("article_id"),
                            "title": item.get("title"),
                            "author": creator,
                            "body": item.get("content"),
                            "source": item.get("source_name"),
                            "published_at": item.get("pubDate"),
                        }

                        cursor.execute("""
                        INSERT INTO articles (id, title, author, body, source, published_at)
                        VALUES (%(id)s, %(title)s, %(author)s, %(body)s, %(source)s, %(published_at)s)
                        ON CONFLICT (id) DO UPDATE SET
                            title = EXCLUDED.title,
                            author = EXCLUDED.author,
                            body = EXCLUDED.body,
                            source = EXCLUDED.source,
                            published_at = EXCLUDED.published_at
                        """, article)
                        inserted_count += 1

                    # Explicitly commit the transaction to persist all inserts
                    conn.commit()
                    print(f"Successfully inserted/updated {inserted_count} articles into the database.")

                # Verification happens in a separate cursor context after commit
                # If verification fails, we know the data was saved
                try:
                    with conn.cursor() as cursor:
                        print("\n--- Verifying data by selecting records ---")
                        cursor.execute("SELECT id, title, source FROM articles LIMIT 5")
                        rows = cursor.fetchall()
                        
                        for row in rows:
                            print(row)
                except psycopg2.Error as verify_error:
                    print(f"Data was saved successfully, but verification query failed: {verify_error}")

        except psycopg2.Error as e:
            print(f"Database error occurred: {e}")

    else:
        print("API request was unsuccessful.")
        print(f"Error details: {response_data}")


if __name__ == "__main__":
    fetch_and_store_articles()
