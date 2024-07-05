-- Data cleaning phase --

SELECT *
FROM layoffs;
-- creating staging table --
CREATE TABLE staging_table
LIKE layoffs;

INSERT INTO staging_table
SELECT *
FROM layoffs;

SELECT *
FROM staging_table;

-- Cheeking and removing duplicates --

SELECT *,
ROW_NUMBER() OVER ( PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM staging_table;

SELECT *
FROM
(
	SELECT *,
ROW_NUMBER() OVER ( PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
	From staging_table
) duplicates
WHERE row_num >1;
-- further checking --
SELECT *
FROM staging_table
WHERE company = 'casper';
-- Removing (deleting) --

WITH duplicates_CTE AS
(
	SELECT *
FROM
(
	SELECT *,
ROW_NUMBER() OVER ( PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
	From staging_table
) duplicates
WHERE row_num >1
)
DELETE
FROM duplicates_CTE;
-- did'nt work--
-- create a new staging table then remove the duplicates --
CREATE TABLE `staging_table2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO staging_table2
(
company, 
location, 
industry, 
total_laid_off, 
percentage_laid_off, 
`date`, 
stage, 
country, 
funds_raised_millions,
row_num
)
SELECT
company, 
location, 
industry, 
total_laid_off, 
percentage_laid_off, 
`date`, 
stage, 
country, 
funds_raised_millions,
ROW_NUMBER() OVER ( PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM staging_table;

SELECT *
FROM staging_table2
WHERE row_num > 1;

DELETE
FROM staging_table2
WHERE row_num > 1;
-- to turn of the safe update --
SET SQL_SAFE_UPDATES = 0;

-- Standardization --

SELECT *
FROM staging_table2;

SELECT  distinct industry
FROM staging_table2;

SELECT company, industry
FROM staging_table2
WHERE industry is null
OR industry = '';

SELECT company, industry
FROM staging_table2
WHERE company like '';

UPDATE staging_table2
SET industry = null
where industry = '';

UPDATE staging_table2 t1
JOIN staging_table2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry is null
AND t2.industry is not null;

SELECT t1.company, t1.industry, t2.industry
FROM staging_table2 t1
JOIN staging_table2 t2
	ON t1.company = t2.company
    AND t1.country = t2.country
WHERE t1.industry is null
AND t2.industry is not null;

SELECT  distinct company, trim(company)
FROM staging_table2;

UPDATE staging_table2
SET company = trim(company);

SELECT distinct location
FROM staging_table2;

SELECT *
FROM staging_table2
WHERE location like '%dorf';
-- seems like 'Dusseldorf' is spelled wrong in another row--
UPDATE staging_table2
SET location = 'Dusseldorf'
WHERE location = 'DÃ¼sseldorf';

UPDATE staging_table2
SET location = 'Florianópolis'
WHERE location ='FlorianÃ³polis';


UPDATE staging_table2
SET location ='Malmo'
WHERE location ='MalmÃ¶';

SELECT distinct industry
FROM staging_table2
order by industry;

UPDATE staging_table2
SET industry = 'Crypto'
WHERE industry like 'cry%';

SELECT distinct country
FROM staging_table2
ORDER BY country;
-- there is 4 rows where united states have . at the end--
UPDATE staging_table2
SET country = 'United States'
WHERE country = 'United States.';

-- Changing the date column type from text to date--
 UPDATE staging_table2
 SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');
 
 ALTER TABLE staging_table2
 MODIFY COLUMN `date` DATE;

select *
from staging_table2;

-- Droping unwanted rows and columns --

DELETE FROM staging_table2
WHERE  total_laid_off is null AND percentage_laid_off is null;

ALTER TABLE staging_table2
DROP COLUMN row_num;



