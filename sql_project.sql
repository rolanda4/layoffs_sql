USE sql_project;

/*looking at a snapshot of our database*/
SELECT *
FROM layoffs;

/*creating a duplicate of the dataset*/
CREATE TABLE layoffs_duplicate
LIKE layoffs;

/*checking that the table has been created with the correct columns*/
SELECT *
FROM layoffs_duplicate;

/*inserting the contents into the duplicate table*/
INSERT layoffs_duplicate
SELECT *
FROM layoffs;

/*checking our duplicated table */
SELECT *
FROM layoffs_duplicate;
/*now we can move forward using the duplicate and leaving the original in case a reference is needed*/

/*checking for duplicates within the dataset using row number over*/
SELECT *, ROW_NUMBER() OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions) AS row_num
FROM layoffs_duplicate;

/*creating a cte to filter for duplicates*/
WITH duplicate_cte AS (
SELECT *, ROW_NUMBER() OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions) AS row_num
FROM layoffs_duplicate
)
SELECT * 
FROM duplicate_cte
WHERE row_num >1;

/*a rown number above 1 depicts a duplicate value*/

/*checking that duplicates are true by using a case study*/
WITH duplicate_cte AS (
SELECT *, ROW_NUMBER() OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions) AS row_num
FROM layoffs_duplicate
)
SELECT * 
FROM duplicate_cte
WHERE company = 'Microsoft';

/*creating a new table that contains only unique rows*/
CREATE TABLE layoffs_dup2(
company TEXT,
location TEXT,
industry TEXT,
total_laid_off INT,
percentage_laid_off TEXT,
date TEXT,
stage TEXT,
country TEXT,
funds_raised_millions INT,
row_num INT
);

INSERT layoffs_dup2
WITH duplicate_cte AS (
SELECT *, ROW_NUMBER() OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions) AS row_num
FROM layoffs_duplicate
)
SELECT * 
FROM duplicate_cte
WHERE row_num = 1;

SELECT *
FROM layoffs_dup2;

/*confirming that all rows have a row_num of 1*/
SELECT DISTINCT row_num
FROM layoffs_dup2;

/*standardizing the data*/

/*removing any white spaces surrounding company*/
UPDATE layoffs_dup2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_dup2
ORDER BY 1;

/*we notice a few issues in the industry column to be addressed
-cryto / crypto currency
-null values
-blanks*/

SELECT *
FROM layoffs_dup2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_dup2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

/*addressing null values*/
/*where do we see the same company in the same location having industry null and not null*/
SELECT *
FROM layoffs_dup2 AS l1
JOIN layoffs_dup2 AS l2
USING (company)
WHERE (l1.industry IS NULL OR l1.industry = '')
AND l2.industry IS NOT NULL;

/*setting blank spaces to null*/
UPDATE layoffs_dup2
SET industry = NULL 
WHERE industry = '';

/*inputing inferred value for industry column from existing data*/
UPDATE layoffs_dup2 AS l1
JOIN layoffs_dup2 AS l2
	USING (company)
SET l1.industry = l2.industry
WHERE l1.industry IS NULL 
AND l2.industry IS NOT NULL;

/*checking that inferable industry null values have been populated*/
SELECT *
FROM layoffs_dup2
WHERE industry IS NULL;

SELECT DISTINCT location
FROM layoffs_dup2
ORDER BY 1;

SELECT DISTINCT country
FROM layoffs_dup2
ORDER BY 1;
/*united states is showing a little discrepancy*/
SELECT *
FROM layoffs_dup2
WHERE country LIKE 'United States%';

UPDATE layoffs_dup2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT DISTINCT country
FROM layoffs_dup2
ORDER BY 1;

/*checking the datatypes of our columns*/
DESCRIBE layoffs_dup2;

/*converting the date to date format*/
UPDATE layoffs_dup2
SET date = STR_TO_DATE(date, '%m/%d/%Y');

/*now that it's in a date format, we can easily convert to date datatype*/
ALTER TABLE layoffs_dup2
MODIFY COLUMN date DATE;

SELECT *
FROM layoffs_dup2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;
/*348 rows have both these key columns null, roughly 15% of our dataset. seeing no viable way to populate
these cells and yet realising not removing them will heavily influence my analysis moving forward. I opt to delete
them. I can always fall back on the original database if need be*/

DELETE
FROM layoffs_dup2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

/*row_num column is now redundant*/
ALTER TABLE layoffs_dup2
DROP COLUMN row_num;


/*EDA*/

/*what date range are we working with in this database*/
SELECT MIN(date),MAX(date)
FROM layoffs_dup2;

/*which companies went completely under */
SELECT industry, COUNT(company)
FROM layoffs_dup2
WHERE percentage_laid_off = 1
GROUP BY industry
ORDER BY 2 DESC;

/*which comapnies are laying off a large number of employees*/
SELECT company,SUM(total_laid_off)
FROM layoffs_dup2
GROUP BY company
ORDER BY 2 DESC;

/*what industry got hit the most*/
SELECT industry,COUNT(company) ,SUM(total_laid_off)
FROM layoffs_dup2
GROUP BY industry
ORDER BY 3 DESC;

/*what countries got hit the most*/
SELECT country,COUNT(country),SUM(total_laid_off)
FROM layoffs_dup2
GROUP BY country
ORDER BY 2 DESC;

/*how did layoffs progress over the years*/
SELECT YEAR(date),COUNT(DISTINCT(MONTH(date))),SUM(total_laid_off)
FROM layoffs_dup2
WHERE date IS NOT NULL
GROUP BY YEAR(date)
ORDER BY 1;

/*finding progression of total layoffs over the period*/
WITH rolling_total AS (
SELECT SUBSTRING(date, 1, 7) AS month, SUM(total_laid_off) AS tots_off
FROM layoffs_dup2
WHERE SUBSTRING(date, 1, 7) IS NOT NULL
GROUP BY month
ORDER BY 1)
SELECT month,tots_off,
SUM(tots_off) OVER(ORDER BY month) AS rolling_total
FROM rolling_total;


/*over the period, which companies have layed the most employees off*/
WITH company_year(company, year,total_layoffs) AS
(
SELECT company, YEAR(date),SUM(total_laid_off)
FROM layoffs_dup2
GROUP BY company,YEAR(date)
ORDER BY company ASC),
company_year_rank AS (SELECT *,
DENSE_RANK() OVER(PARTITION BY year ORDER BY total_layoffs DESC) AS ranking
FROM company_year
WHERE year IS NOT NULL)
SELECT *
FROM company_year_rank
WHERE ranking <= 5
ORDER BY year;

/*does the stage a company is in affect layoffs*/
SELECT stage,SUM(total_laid_off)
FROM layoffs_dup2
GROUP BY stage
ORDER BY 2 DESC;
