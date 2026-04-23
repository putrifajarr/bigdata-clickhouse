SELECT 
    leftPad(toString(cik_str), 10, '0') AS cik_clean,
    title,
    language,
    text,
    url,
    ts
FROM news_scrape_data_v2
WHERE language = 'en' 
  AND title != '' 
  AND cik_str != '' 
  AND cik_str IS NOT NULL
LIMIT 10;