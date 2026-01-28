CREATE OR REPLACE VIEW movies_underrated AS
SELECT
    movie_id,
    title,
    year,
    nb_ratings,
    avg_rating,
    ROUND(avg_rating * SQRT(nb_ratings), 2) as potential_score
FROM movies_stats
WHERE nb_ratings BETWEEN 5 AND 5000
  AND avg_rating >= 3.5
ORDER BY potential_score DESC;
