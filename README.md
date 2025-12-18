# 🎬 Movie Data Pipeline (ETL Project)

## 📌 Project Overview
This project implements a simple end-to-end **data pipeline** for movie analytics.  
The pipeline extracts movie and rating data from the **MovieLens dataset**, enriches it using the **OMDb API**, transforms and cleans the data, and loads it into a **PostgreSQL** relational database.  
Finally, analytical SQL queries are used to generate insights for the analytics team.

This project demonstrates core **Data Engineering concepts**:
- ETL (Extract, Transform, Load)
- API integration
- Data modeling & normalization
- Idempotent database loading
- Analytical SQL querying

---

## 📂 Data Sources
### 1. MovieLens Dataset (Local CSV)
- Files used:
  - `movies.csv`
  - `ratings.csv`
- Source: https://grouplens.org/datasets/movielens/latest/

### 2. OMDb API (External API)
- Provides additional movie details:
  - Director
  - Plot
  - Box Office earnings
- API website: http://www.omdbapi.com/
- A free API key is required

---

## 🗄️ Database & Schema Design
**Database Used:** PostgreSQL

### Tables:
- `movies` – Stores movie metadata
- `genres` – Stores unique genres
- `movie_genres` – Many-to-many mapping between movies and genres
- `ratings` – Stores user ratings

### Key Design Decisions:
- Normalized schema to avoid data duplication
- Many-to-many relationship for movies and genres
- Primary keys and foreign keys ensure data integrity
- Idempotent inserts using `ON CONFLICT DO NOTHING`

---

## ⚙️ Technology Stack
- **Language:** Python 3
- **Libraries:** pandas, SQLAlchemy, requests
- **Database:** PostgreSQL
- **API:** OMDb API

---

## 🚀 Setup Instructions

### 1️⃣ Install Dependencies
```bash
pip install pandas sqlalchemy psycopg2-binary requests
