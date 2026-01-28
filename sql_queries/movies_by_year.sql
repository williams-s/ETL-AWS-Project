CREATE OR REPLACE VIEW movies_by_year AS
SELECT
    year,
    COUNT(*) as total_movies,
    SUM(nb_ratings) as total_ratings,
    ROUND(AVG(avg_rating), 2) as avg_rating_year,
    ROUND(AVG(rating_stddev), 2) as avg_stddev_year
FROM movies_stats
WHERE year IS NOT NULL
GROUP BY year
ORDER BY year;
