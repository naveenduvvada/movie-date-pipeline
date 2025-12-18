CREATE TABLE movies (
    movie_id SERIAL PRIMARY KEY,
    title VARCHAR(255),
    release_year INT
);

CREATE TABLE genres (
    genre_id SERIAL PRIMARY KEY,
    genre_name VARCHAR(100)
);

CREATE TABLE ratings (
    rating_id SERIAL PRIMARY KEY,
    movie_id INT REFERENCES movies(movie_id),
    rating FLOAT
);
