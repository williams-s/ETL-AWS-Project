CREATE OR REPLACE VIEW top_movies AS
SELECT
    movie_id,
    title,
    year,
    nb_ratings,
    avg_rating,
    rating_stddev,
    ROW_NUMBER() OVER (ORDER BY nb_ratings DESC) as rank_popularity,
    ROW_NUMBER() OVER (ORDER BY avg_rating DESC) as rank_avg_rating
FROM movies_stats
WHERE nb_ratings >= 10 and avg_rating > 3.2
ORDER BY nb_ratings DESC;
