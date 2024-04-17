WITH CategoryAverages AS (
    SELECT
        cat.cat_name,
        AVG(ch.count) AS avg_checkins,
        AVG(b.stars) AS avg_stars
    FROM
        Checkins ch
        JOIN Categories cat ON ch.business_id = cat.business_id
        JOIN Business b ON ch.business_id = b.business_id
    GROUP BY
        cat.cat_name
),
FirstCategoryPerBusiness AS (
    SELECT
        b.business_id,
        b.name,
        MIN(cat.cat_name) AS first_category
    FROM
        Business b
        JOIN Categories cat ON b.business_id = cat.business_id
    GROUP BY
        b.business_id,
        b.name
),
BusinessCheckins AS (
    SELECT
        ch.business_id,
        SUM(ch.count) AS total_checkins
    FROM
        Checkins ch
    GROUP BY
        ch.business_id
),
BusinessWithFirstCategoryAndCheckins AS (
    SELECT
        fc.business_id,
        fc.name,
        fc.first_category,
        b.stars,
        bc.total_checkins,
        b.zipcode
    FROM
        FirstCategoryPerBusiness fc
        JOIN BusinessCheckins bc ON fc.business_id = bc.business_id
        JOIN Business b ON fc.business_id = b.business_id
)
SELECT
    bfc.business_id,
    bfc.name,
    bfc.first_category,
    bfc.stars,
    bfc.total_checkins
FROM
    BusinessWithFirstCategoryAndCheckins bfc
    JOIN CategoryAverages ca ON bfc.first_category = ca.cat_name
WHERE
    bfc.total_checkins > ca.avg_checkins
    AND bfc.stars > ca.avg_stars
    AND bfc.zipcode = '{zipcode}'
ORDER BY
    bfc.first_category, bfc.name;
