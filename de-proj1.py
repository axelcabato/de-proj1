# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.17.3
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# ## Python NewsData.io API Connection & Info Processing Script

# %%
import sqlite3
from newsdataapi import NewsDataApiClient


# Replace with your actual API key
API_KEY = "pub_ac6cd23aa0124ba197f63ad7a78f295d"   # actual
DB_NAME = "newsdata.db"

def fetch_and_store_articles():
    """
    Fetches news articles from NewsData.io API and stores them in an SQLite database.
    """
    try:
        api = NewsDataApiClient(apikey=API_KEY)
        response_data = api.news_api(q="data engineering", language="en")
    except Exception as e:
        print(f"Failed to fetch data from API: {e}")
        return

    # Upon successful API call, properly format and store deired data
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
                
                print(f"Successfully inserted/updated {inserted_count} articles into the database.")
                
                print("\n--- Verifying data by selecting records ---")
                cursor.execute("SELECT id, title, source FROM articles LIMIT 5")
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

# %% [markdown]
# ## Extract  

# %%
API_KEY = "pub_ac6cd23aa0124ba197f63ad7a78f295d"   # actual
DB_NAME = "newsdata.db"

try:
    api = NewsDataApiClient(apikey=API_KEY)
    response_data = api.news_api(q="data engineering", language="en")
except Exception as e:
    print(f"Failed to fetch data from API: {e}")

# %% [markdown]
# ## Transform + Load + Validation + Error Handling

# %%
# --- DATA PROCESSING & VALIDATION ---

# Check if the API request was successful and if there are articles to process.
if response_data and response_data.get("status") == "success":
    # Extracts the list of articles from the API response.
    
    articles_to_store = response_data.get("results", [])  # `[]` is a default value to prevent errors if "results" is missing.

    # Checks if `articles_to_store` list is empty and prints a message if so.
    if not articles_to_store:
        print("No articles found to store.")

    print(f"Successfully fetched {len(articles_to_store)} articles.")
    print("--- Storing data in database ---")

    # --- DATABASE OPERATIONS ---

    try:
        # Establishes a connection to the SQLite database.
        with sqlite3.connect(DB_NAME) as conn:  # with` statement ensures the connection is automatically closed.

            # Creates a cursor object to execute SQL commands.
            cursor = conn.cursor()

            # Creates the `articles` table if it doesn't already exist.
            # `TEXT PRIMARY KEY` ensures each article has a unique ID.
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
            # Loops through each article to prepare it for insertion.
            for item in articles_to_store:
                # Retrieves the `creator` field from the article data.
                creator = item.get("creator")
                
                # IMPORTANT: Handles cases where `creator` is a list (multiple authors). Joining the list items into a single string separated by a comma.
                if isinstance(creator, list):
                    creator = ", ".join(creator)
                
                # Maps the API fields to the database table columns.
                article = {
                    "id": item.get("article_id"),
                    "title": item.get("title"),
                    "author": creator,  # Uses the cleaned creator data
                    "body": item.get("content"),
                    "source": item.get("source_name"),
                    "published_at": item.get("pubDate"),
                }
                
                # Inserts the prepared article data into the database.
                cursor.execute("""
                INSERT OR REPLACE INTO articles (id, title, author, body, source, published_at)
                VALUES (:id, :title, :author, :body, :source, :published_at)
                """, article)  
                # 'INSERT OR REPLACE' either inserts a new row or replaces an existing one if the `id` already exists.
                inserted_count += 1
            
            print(f"Successfully inserted/updated {inserted_count} articles into the database.")
            
            # --- DATA VERIFICATION ---

            print("\n--- Verifying data by selecting records ---")
            # Selects and displays the first 5 records to confirm they were stored correctly.
            cursor.execute("SELECT id, title, source FROM articles LIMIT 5")
            rows = cursor.fetchall()

            for row in rows:
                print(row)

    
    # Catches any database-related errors and prints a descriptive message.
    except sqlite3.Error as e:
        print(f"Database error occurred: {e}")

# --- ERROR HANDLING FOR API REQUEST ---
else:
    # executes if the initial API call was not successful.
    print("API request was unsuccessful.")
    print(f"Error details: {response_data}")
