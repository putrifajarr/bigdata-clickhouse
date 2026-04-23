SELECT 
    company_name AS original_company,
    trim(replaceRegexpAll(lower(company_name), '(/fl|/new|/de|/inc|/corp)$', '')) AS cleaned_company,
    text AS original_text,
    replaceRegexpAll(replaceRegexpAll(text, '<[^>]+>', ''), '&[a-z#0-9]+;', '') AS cleaned_text,
    cik_str AS original_cik,
    leftPad(cik_str, 10, '0') AS cleaned_cik
FROM praktikum.scrape_data_sample