SELECT 
    -- Testing Point 2 & 5 (Company Name)
    company_name AS original_company,
    trim(replaceRegexpAll(lower(company_name), '(/fl|/new|/de)$', '')) AS cleaned_company,

    -- Testing Point 8 (HTML Stripping)
    text AS original_text,
    replaceRegexpAll(replaceRegexpAll(text, '<[^>]+>', ''), '&[a-z#0-9]+;', '') AS cleaned_text,

    -- Testing Point 3 (CIK Padding)
    cik_str AS original_cik,
    leftPad(cik_str, 10, '0') AS cleaned_cik
FROM news_staging;
LIMIT 100; -- Gunakan limit untuk testing di Step 2 Docker