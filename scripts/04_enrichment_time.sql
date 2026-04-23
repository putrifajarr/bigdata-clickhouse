ENGINE = MergeTree()
ORDER BY (ticker, ts)
AS
SELECT
    *,
    toHour(ts) AS hour,
    toMinute(ts) AS minute,
    toDayOfWeek(ts) AS day_of_week,
    if(toDayOfWeek(ts) IN (6, 7), 1, 0) AS is_weekend,
    row_number() OVER (
        PARTITION BY url, ticker
        ORDER BY scraped_at DESC
    ) AS rn
FROM praktikum.scrape_data_sample;