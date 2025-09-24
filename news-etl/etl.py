import os
import sqlite3
from newsdataapi import NewsDataApiClient

# Read the API key from an environment variable
API_KEY: str | None = os.environ.get("NEWS_API_KEY")

if not API_KEY:
    print("Error: NEWS_API_KEY environment variable not set.")
    exit(1)

# Type checker hint: API_KEY is now guaranteed to be str
api = NewsDataApiClient(apikey=API_KEY)  # type: ignore

DB_NAME = "newsdata.db"


def fetch_and_store_articles():
    """
    Fetches news articles from NewsData.io API and stores them in an SQLite database.
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
            with sqlite3.connect(DB_NAME) as conn:
                cursor = conn.cursor()

                cursor.execute("""
                CREATE TABLE IF NOT EXISTS articles (
                    id TEXT PRIMARY KEY,
                    title TEXT,
                    author TEXT,
                    body TEXT,
                    source TEXT,
                    published_at TEXT
                )
                """)

                inserted_count = 0
                for item in articles_to_store:
                    # SOLUTION: Check if the 'creator' is a list and join it into a string
                    creator = item.get("creator")
                    if isinstance(creator, list):
                        # Join the list of creators into a single string
                        creator = ", ".join(creator)

                    article = {
                        "id": item.get("article_id"),
                        "title": item.get("title"),
                        "author": creator,  # Use the corrected creator variable
                        "body": item.get("content"),
                        "source": item.get("source_name"),
                        "published_at": item.get("pubDate"),
                    }

                    cursor.execute("""
                    INSERT OR REPLACE INTO articles (id, title, author, body, source, published_at)
                    VALUES (:id, :title, :author, :body, :source, :published_at)
                    """, article)
                    inserted_count += 1

                print(
                    f"Successfully inserted/updated {inserted_count} articles into the database.")

                print("\n--- Verifying data by selecting records ---")
                cursor.execute(
                    "SELECT id, title, source FROM articles LIMIT 5")
                rows = cursor.fetchall()

                for row in rows:
                    print(row)

        except sqlite3.Error as e:
            print(f"Database error occurred: {e}")

    else:
        print("API request was unsuccessful.")
        print(f"Error details: {response_data}")


if __name__ == "__main__":
    fetch_and_store_articles()
