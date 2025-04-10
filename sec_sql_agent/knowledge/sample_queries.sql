-- Here are some sample queries for SEC filings analysis

-- <query description>
-- Get a company's revenue over time
-- </query description>
-- <query>
SELECT fiscal_year, fiscal_period, value_text, period_end_date
FROM financial_data
WHERE ticker = 'NVDA'
  AND concept = 'Revenues'
  AND statement_type = 'Income Statement'
ORDER BY period_end_date;
-- </query>

-- <query description>
-- Calculate quarter-over-quarter revenue growth
-- </query description>
-- <query>
WITH quarterly_revenue AS (
  SELECT
    fiscal_year,
    fiscal_period,
    period_end_date,
    SUM(value_numeric) as revenue
  FROM financial_data
  WHERE ticker = 'NVDA'
    AND concept = 'Revenues'
    AND statement_type = 'Income Statement'
  GROUP BY fiscal_year, fiscal_period, period_end_date
  ORDER BY period_end_date
)
SELECT
  current.fiscal_year,
  current.fiscal_period,
  current.period_end_date,
  current.revenue as current_revenue,
  previous.revenue as previous_revenue,
  (current.revenue - previous.revenue) / previous.revenue * 100 as growth_percentage
FROM quarterly_revenue current
LEFT JOIN quarterly_revenue previous
  ON previous.period_end_date = (
    SELECT MAX(period_end_date)
    FROM quarterly_revenue
    WHERE period_end_date < current.period_end_date
  )
ORDER BY current.period_end_date;
-- </query>

-- <query description>
-- Compare total assets across companies for the most recent quarter
-- </query description>
-- <query>
SELECT
  fd.ticker,
  c.name as company_name,
  fd.value_text,
  fd.as_of_date
FROM financial_data fd
JOIN companies c ON fd.ticker = c.ticker
WHERE fd.concept = 'Assets'
  AND fd.statement_type = 'Balance Sheet'
  AND fd.fiscal_period = 'Q3'
  AND fd.fiscal_year = 2023;
-- </query>

-- <query description>
-- Get management discussion text block
-- </query description>
-- <query>
SELECT content
FROM text_blocks
WHERE ticker = 'NVDA'
  AND fiscal_year = 2023
  AND fiscal_period = 'annual'
  AND title LIKE '%Management Discussion%';
-- </query>

-- <query description>
-- Calculate gross margin percentage over time
-- </query description>
-- <query>
SELECT
  fd.fiscal_year,
  fd.fiscal_period,
  fd.period_end_date,
  SUM(CASE WHEN fd.concept = 'Revenues' THEN fd.value_numeric ELSE 0 END) as revenue,
  SUM(CASE WHEN fd.concept = 'Cost Of Revenue' THEN fd.value_numeric ELSE 0 END) as cost_of_revenue,
  SUM(CASE WHEN fd.concept = 'Revenues' THEN fd.value_numeric ELSE 0 END) -
    SUM(CASE WHEN fd.concept = 'Cost Of Revenue' THEN fd.value_numeric ELSE 0 END) as gross_profit,
  (SUM(CASE WHEN fd.concept = 'Revenues' THEN fd.value_numeric ELSE 0 END) -
    SUM(CASE WHEN fd.concept = 'Cost Of Revenue' THEN fd.value_numeric ELSE 0 END)) /
    SUM(CASE WHEN fd.concept = 'Revenues' THEN fd.value_numeric ELSE 0 END) * 100 as gross_margin_percentage
FROM financial_data fd
WHERE fd.ticker = 'NVDA'
  AND fd.statement_type = 'Income Statement'
  AND fd.concept IN ('Revenues', 'Cost Of Revenue')
GROUP BY fd.fiscal_year, fd.fiscal_period, fd.period_end_date
ORDER BY fd.period_end_date;
-- </query>

-- <query description>
-- Find all mentions of "AI" or "artificial intelligence" in text blocks
-- </query description>
-- <query>
SELECT
  tb.ticker,
  c.name as company_name,
  tb.fiscal_year,
  tb.fiscal_period,
  tb.title,
  tb.content
FROM text_blocks tb
JOIN companies c ON tb.ticker = c.ticker
WHERE tb.content ILIKE '%artificial intelligence%'
   OR tb.content ILIKE '%AI %'
ORDER BY tb.ticker, tb.fiscal_year, tb.fiscal_period;
-- </query>

-- <query description>
-- Compare R&D expenses across companies
-- </query description>
-- <query>
SELECT
  fd.ticker,
  c.name as company_name,
  fd.fiscal_year,
  fd.fiscal_period,
  fd.period_end_date,
  fd.value_numeric as rd_expense
FROM financial_data fd
JOIN companies c ON fd.ticker = c.ticker
WHERE fd.concept LIKE '%Research And Development%'
  AND fd.statement_type = 'Income Statement'
  AND fd.fiscal_period = 'annual'
ORDER BY fd.ticker, fd.period_end_date;
-- </query>
