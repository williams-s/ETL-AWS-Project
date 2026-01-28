CREATE OR REPLACE VIEW movies_stats AS
SELECT 
    m.movie_id,
    m.title,
    m.year,
    COUNT(r.rating) as nb_ratings,
    ROUND(AVG(r.rating), 2) as avg_rating,
    MIN(r.rating) as min_rating,
    MAX(r.rating) as max_rating,
    ROUND(STDDEV(r.rating), 2) as rating_stddev,
    MIN(r.rating_date) as first_rating_date,
    MAX(rating_date) as last_rating_date
    
FROM movies m
LEFT JOIN ratings r ON m.movie_id = r.movie_id
GROUP BY m.movie_id, m.title, m.year;
