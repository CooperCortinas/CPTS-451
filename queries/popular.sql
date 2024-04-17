WITH MonthlyReviews AS (
    SELECT
        business_id,
        DATE_TRUNC('month', date) AS review_month,
        COUNT(*) AS monthly_reviews
    FROM
        Review
    GROUP BY
        business_id, review_month
),
MonthlyGrowth AS (
    SELECT
        mr.business_id,
        mr.review_month,
        mr.monthly_reviews,
        LAG(mr.monthly_reviews, 1) OVER(PARTITION BY mr.business_id ORDER BY mr.review_month) AS prev_month_reviews,
        (mr.monthly_reviews - LAG(mr.monthly_reviews, 1) OVER(PARTITION BY mr.business_id ORDER BY mr.review_month))::FLOAT / NULLIF(LAG(mr.monthly_reviews, 1) OVER(PARTITION BY mr.business_id ORDER BY mr.review_month), 0) AS growth_rate
    FROM
        MonthlyReviews mr
)
SELECT
    mg.business_id,
    b.name,
    AVG(mg.growth_rate) AS avg_monthly_growth_rate
FROM
    MonthlyGrowth mg
JOIN
    Business b ON mg.business_id = b.business_id
WHERE
    mg.prev_month_reviews IS NOT NULL
    AND b.zipcode = '{zipcode}'
GROUP BY
    mg.business_id, b.name
HAVING
    AVG(mg.growth_rate) > 0.329
ORDER BY
    avg_monthly_growth_rate DESC;
