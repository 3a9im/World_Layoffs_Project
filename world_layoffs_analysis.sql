-- Analysis phase --
SELECT * 
FROM staging_table2;
-- exploring laid offs by company --
-- 1. single layoff --
SELECT company, total_laid_off
FROM staging_table2
ORDER BY 2 DESC
LIMIT 5;

-- 2. total layoffs --
SELECT company, sum(total_laid_off) AS sum_laid_off
FROM staging_table2
GROUP BY company
ORDER BY sum_laid_off DESC
LIMIT 5;

-- by location
SELECT location, SUM(total_laid_off)
FROM staging_table2
GROUP BY location
ORDER BY 2 DESC
LIMIT 10;

-- by industry --
SELECT industry, sum(total_laid_off) AS sum_laid_off
FROM staging_table2
GROUP BY industry
ORDER BY sum_laid_off DESC;

-- by year --
SELECT year(`date`) as years, sum(total_laid_off) AS sum_laid_off
FROM staging_table2
GROUP BY years
ORDER BY sum_laid_off DESC;

-- by month --
SELECT month(`date`) as months, sum(total_laid_off) AS sum_laid_off
FROM staging_table2
GROUP BY months
ORDER BY sum_laid_off DESC;

-- by the stage the company is in --
SELECT stage, sum(total_laid_off) AS sum_laid_off
FROM staging_table2
GROUP BY stage
ORDER BY sum_laid_off DESC;

-- and finally by country --
SELECT country, sum(total_laid_off) AS sum_laid_off
FROM staging_table2
GROUP BY country
ORDER BY sum_laid_off DESC;

-- exploring the percentages of laid off --
SELECT MAX(percentage_laid_off), MIN(percentage_laid_off)
FROM staging_table2
WHERE percentage_laid_off IS NOT NULL;

-- 1 = 100% means the compaines went out of business let's explore those --
SELECT company, percentage_laid_off
FROM staging_table2
WHERE percentage_laid_off = 1;
-- 116 compaines went out --
SELECT *
FROM staging_table2
WHERE  percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

SELECT MAX(total_laid_off), MIN(total_laid_off)
FROM staging_table2;

-- compaines with most layoff during a specific year --
WITH Company_Year AS 
(
  SELECT company, YEAR(date) AS years, SUM(total_laid_off) AS total_laid_off
  FROM layoffs_staging2
  GROUP BY company, YEAR(date)
)
, Company_Year_Rank AS (
  SELECT company, years, total_laid_off, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
  FROM Company_Year
)
SELECT company, years, total_laid_off, ranking
FROM Company_Year_Rank
WHERE ranking <= 3
AND years IS NOT NULL
ORDER BY years ASC, total_laid_off DESC;


-- Rolling Total of Layoffs Per Month
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM staging_table2
GROUP BY dates
ORDER BY dates ASC;

-- now use it in a CTE so we can query off of it
WITH DATE_CTE AS 
(
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM staging_table2
GROUP BY dates
ORDER BY dates ASC
)
SELECT dates, SUM(total_laid_off) OVER (ORDER BY dates ASC) as rolling_total_layoffs
FROM DATE_CTE
ORDER BY dates ASC;