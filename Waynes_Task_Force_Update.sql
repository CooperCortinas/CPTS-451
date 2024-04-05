-- Calculate num_checkins for business table
UPDATE Business
SET num_checkins = COALESCE((
    SELECT SUM(count) FROM Checkins
    WHERE Checkins.business_id = Business.business_id
), 0);

-- Create a temporary table for aggregate review data
CREATE TEMP TABLE IF NOT EXISTS temp_aggregate AS
SELECT 
    business_id,
    COALESCE(COUNT(*), 0) AS review_count,
    COALESCE(AVG(stars), 0) AS review_rating
FROM Review
GROUP BY business_id;

-- Update the Business table using the temporary table for aggregate review data
UPDATE Business
SET 
    review_count = temp_aggregate.review_count,
    review_rating = temp_aggregate.review_rating
FROM temp_aggregate
WHERE Business.business_id = temp_aggregate.business_id;