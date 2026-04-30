INSERT INTO praktikum.final_pipeline
WITH

cleaned AS (
    SELECT 
        company_name AS original_company,
        trim(replaceRegexpAll(lower(company_name), '(/fl|/new|/de|/inc|/corp)$', '')) AS cleaned_company,

        text AS original_text,
        replaceRegexpAll(
            replaceRegexpAll(text, '<[^>]+>', ''), 
            '&[a-z#0-9]+;', ''
        ) AS cleaned_text,

        cik_str AS original_cik,
        leftPad(cik_str, 10, '0') AS cleaned_cik,

        title,
        language,
        url,
        ts

    FROM praktikum.news_scrape_data_v2
),

standardized AS (
    SELECT
        cleaned_cik AS cik_clean,
        cleaned_company,
        cleaned_text AS text,
        title,
        language,
        url,
        ts
    FROM cleaned
    WHERE 
        rn = 1
        AND text IS NOT NULL
        AND length(text) > 0
        AND cik_clean IS NOT NULL
),

features AS (
    SELECT
        cik_clean,
        cleaned_company,
        title,
        text,
        url,
        ts,
        toHour(ts) AS hour,
        toMinute(ts) AS minute,
        toDayOfWeek(ts) AS day_of_week,
        if(toDayOfWeek(ts) IN (6, 7), 1, 0) AS is_weekend,

        row_number() OVER (
            PARTITION BY url, cik_clean
            ORDER BY ts DESC
        ) AS rn

    FROM standardized
),

dedup AS (
    SELECT *
    FROM features
    WHERE (url, cik_clean, ts) IN (
        SELECT 
            url, 
            cik_clean, 
            max(ts)
        FROM features
        GROUP BY url, cik_clean
    )
)

SELECT * FROM final