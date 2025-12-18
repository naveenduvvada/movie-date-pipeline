import pandas as pd
import requests
from sqlalchemy import create_engine, text
import time

# ==============================
# CONFIGURATION
# ==============================
# Update these values before running
DB_URL = "postgresql://postgres:7386@localhost:5432/moviedb"  # Correct format: username:password@host:port/dbname
OMDB_API_KEY = "33fdf3b7"

MOVIES_CSV = "movies.csv"
RATINGS_CSV = "ratings.csv"

# ==============================
# DATABASE CONNECTION
# ==============================
print("Connecting to database...")
try:
    engine = create_engine(DB_URL)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        print("Database connection successful!", result.fetchone())
except Exception as e:
    print("Database connection failed:", e)
    exit()

# ==============================
# OMDb API FUNCTION
# ==============================
def fetch_omdb_data(title, year=None):
    """
    Fetch movie details from OMDb API.
    Returns None if movie is not found.
    """
    params = {
        "t": title,
        "apikey": OMDB_API_KEY
    }

    if year:
        params["y"] = year

    try:
        response = requests.get("http://www.omdbapi.com/", params=params, timeout=5)
        if response.status_code == 200:
            data = response.json()
            if data.get("Response") == "True":
                return data
            else:
                print(f"OMDb not found for: {title} ({year})")
    except Exception as e:
        print(f"OMDb request failed for {title}: {e}")

    return None

# ==============================
# EXTRACT: READ CSV FILES
# ==============================
print("Reading CSV files...")
try:
    movies_df = pd.read_csv(MOVIES_CSV)
    ratings_df = pd.read_csv(RATINGS_CSV)
    print(f"Movies loaded: {len(movies_df)} rows")
    print(f"Ratings loaded: {len(ratings_df)} rows")
except FileNotFoundError as e:
    print("CSV file not found:", e)
    exit()

# ==============================
# TRANSFORM: CLEAN MOVIES DATA
# ==============================
print("Cleaning movie titles and extracting release year...")
movies_df["release_year"] = movies_df["title"].str.extract(r"\((\d{4})\)")
movies_df["clean_title"] = movies_df["title"].str.replace(r"\s\(\d{4}\)", "", regex=True)

# ==============================
# TRANSFORM + ENRICH: OMDb API
# ==============================
print("Fetching data from OMDb API (first 5 movies for testing)...")
movies_enriched = []

for _, row in movies_df.head(5).iterrows():  # Test first 5 movies
    print(f"Processing: {row['clean_title']} ({row['release_year']})")
    omdb_data = fetch_omdb_data(row["clean_title"], row["release_year"])

    movie_record = {
        "movie_id": int(row["movieId"]),
        "title": row["clean_title"],
        "release_year": int(row["release_year"]) if pd.notna(row["release_year"]) else None,
        "director": omdb_data.get("Director") if omdb_data else None,
        "plot": omdb_data.get("Plot") if omdb_data else None,
        "box_office": omdb_data.get("BoxOffice") if omdb_data else None,
        "genres": row["genres"].split("|")
    }

    # BONUS FEATURE ENGINEERING: decade
    if movie_record["release_year"]:
        movie_record["decade"] = (movie_record["release_year"] // 10) * 10
    else:
        movie_record["decade"] = None

    movies_enriched.append(movie_record)

    # Delay to avoid API rate limits
    time.sleep(0.2)

print(f"Movies enriched: {len(movies_enriched)}")

# ==============================
# TRANSFORM: RATINGS DATA
# ==============================
print("Transforming ratings timestamps...")
ratings_df["rating_timestamp"] = pd.to_datetime(ratings_df["timestamp"], unit="s")

# ==============================
# LOAD: INSERT DATA INTO DATABASE
# ==============================
print("Loading data into database (first 5 movies)...")

with engine.begin() as conn:
    # Insert movies
    for movie in movies_enriched:
        try:
            conn.execute(text("""
                INSERT INTO movies (movie_id, title, release_year, director, plot, box_office)
                VALUES (:movie_id, :title, :release_year, :director, :plot, :box_office)
                ON CONFLICT (movie_id) DO NOTHING
            """), movie)
        except Exception as e:
            print(f"Failed to insert movie {movie['title']}: {e}")

        # Insert genres and mapping
        for genre in movie["genres"]:
            try:
                conn.execute(text("""
                    INSERT INTO genres (genre_name)
                    VALUES (:genre)
                    ON CONFLICT (genre_name) DO NOTHING
                """), {"genre": genre})

                conn.execute(text("""
                    INSERT INTO movie_genres (movie_id, genre_id)
                    SELECT :movie_id, genre_id
                    FROM genres
                    WHERE genre_name = :genre
                    ON CONFLICT DO NOTHING
                """), {"movie_id": movie["movie_id"], "genre": genre})
            except Exception as e:
                print(f"Failed to insert genre mapping for {movie['title']} - {genre}: {e}")

    # Insert ratings (first 10 for testing)
    try:
        ratings_df.head(10)[["userId", "movieId", "rating", "rating_timestamp"]].to_sql(
            "ratings",
            engine,
            if_exists="append",
            index=False
        )
    except Exception as e:
        print("Failed to insert ratings:", e)

print("ETL pipeline test completed successfully!")