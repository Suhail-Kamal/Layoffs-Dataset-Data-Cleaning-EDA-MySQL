                      -- SQL Project - Data Cleaning

				-- https://www.kaggle.com/datasets/swaptr/layoffs-2022


SELECT * 
FROM layoffs;


-- first thing i want to do is create a staging table. This is the one i will work in and clean the data. I want a table with the raw data in case something happens

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT * 
FROM layoffs;

INSERT layoffs_staging
SELECT * 
FROM layoffs;

-- These are the steps iam going to follow for cleaning the Data
-- 1. Check for duplicates and removes any
-- 2. Standardize the Data and fix errors
-- 3. Look at Null Values or Blank Values and see what
-- 4. Remove Any Columns and rows that are not necessary


-- 1. Remove Duplicates
-- Lets identify duplicates

SELECT * ,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS
(
SELECT * ,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte 
WHERE row_num >1;

-- Lets look at this company 'Oda'

SELECT *
FROM layoffs_staging
WHERE company = 'Oda';

-- it found out to be these are not duplicates because only upto the chosen columns in the query is same and rest of them arent the same
-- so i want to change my query for finding out the duplicates by putting all the column names in the query

WITH duplicate_cte AS
(
SELECT * ,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte 
WHERE row_num >1;

-- Lets look at this company 'Casper'


SELECT *
FROM layoffs_staging
WHERE company LIKE 'Casper';
-- Its found out that 'casper' company having 2 same row of entries which is the duplicate and i want to remove only one from that ie only removing the duplicate entry


WITH duplicate_cte AS
(
SELECT * ,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
DELETE  
FROM duplicate_cte 
WHERE row_num >1;
-- After running this query its saying 'The target table duplicate_cte of the DELETE is not updatabe' , means i cannot update an cte table bcz a DELETE statement is similar to an UPDATE statement
-- so what iam going to do now is Creating a layoffs_staging2 table which will have row_num1 column and then deleting the rows where the row_num1 >1

-- So lets create a table

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num1` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
-- lets check whether its worked or not
SELECT * 
FROM layoffs_staging2;
-- yes it worked properly, so now i have this empty table and i want to insert the informations now

INSERT INTO layoffs_staging2
SELECT * ,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;
-- run this to check whether its worked and yes its worked
-- lets look what are the informations in this new table by giving the below query

SELECT * 
FROM layoffs_staging2;
-- in this new table we added one more column which is row_num1 so it is easy now for me to delete the duplicates by giving a filter in the query and below is the query

SELECT *
FROM layoffs_staging2
WHERE row_num1 >1;
-- now i got the duplicates(5 rows returned) and i want to delete it by giving the below query

DELETE 
FROM layoffs_staging2
WHERE row_num1 >1;
-- now lets check whether the duplicates entries are deleted or not by giving the below query

SELECT *
FROM layoffs_staging2
WHERE row_num1 >1;
-- 0 rows returned which means there is no duplicates now 

-- now lets look at the layoffs_staging2 table
SELECT *
FROM layoffs_staging2;
-- now i actually not needed the row_num1 column so i can get rid of them by giving a drop query at the end.
-- Thats how i deleted the duplicates now lets look at standardizing data


-- 2. Standardizing Data

-- finding issues in the data and fixing it

SELECT *
FROM layoffs_staging2;

-- found out to be there are white spaces in the data so lets use the TRIM function 
-- TRIM just takes off the white spaces at the end and beginning
SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

-- lets check whether it is update or not using the below query
SELECT *
FROM layoffs_staging2;
-- its updated successfully, now lets look at the industry

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;
-- its found that Crypto,Crypto Currency,CryptoCurrency are all of the same thing so these are all labelled exactly same ie 'Crypto', so i need to fix it by using the below query
-- if i didnt fix it now, these three will be of their unique rows or their unique thing so i want them to be all together so that i can accurately look at the data
-- Lets update it now 

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Lets see whether it is updated or not 
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';
-- its updated beautifully
-- now lets look at the location

SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1;
-- location looks good, now lets look at the country

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;
-- there is a problem, that country name is 'United States.' so i need to get rid of the '.'
-- lets fix it 

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2;
-- now i want to update it on the country column, so lets do it

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';
-- lets run this and check whether it is updated or not using the below query

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2;
-- its updated because now there is only one row of United States

-- the date column iam having is in text column so i want to change it to date column
SELECT `date`
FROM layoffs_staging2;

-- lets fix it

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;
-- lets update this `date` column into like this 'STR_TO_DATE(`date`, '%m/%d/%Y')'

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');
-- run this and checking it using the below query

SELECT `date`
FROM layoffs_staging2;
-- now it looks proper
-- now i want to change the date column from text format to an date column format
-- So lets fix it

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;
-- now the date column in text format is changed to date format

-- 3. NULL valuses or Blank Values

-- There are some NULL/Blank values in the industry column. so lets have a look at that.
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';
-- so there are few industries which are blank so lets check any of the company is populated or not
-- lets take Airbnb as an example

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';
-- its showing that company Airbnb is an Travel industry so what i need to do is i want populate it on the blank values in the industry column 
-- similary for the remaining blank values of industries i need to populate their appropriate industries
-- So before populating i want to set the blank values in the industry column to NULL
-- So lets do that 

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';
-- Run this and check using the below query

SELECT t1.industry,t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
  ON t1.company = t2.company
  AND t1.location = t2.location
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

-- what iam going to do is to populate industry in the t1.industry column which is NULL, with the industry from the t2.industry column where any one of the row is not NULL in that company
-- Lets do that

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
  ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;
-- Run this and lets check whether it is updated or not using the below query using an example

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';
-- companies like Airbnb,Juul,Carvana industry is populated but company named Bally's Interactive is not populated so lets check that one.

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';
-- since there is only one row for this company which means there isnt any multiple layoffs so i dont have another populated row that's not NULL to populate this row.
-- so Data cleaning for the Blank/NULL values are done now

-- 4. Remove any Rows/Columns
-- Now i want to remove the Rows/Columns that are not necessary
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;
-- these results from this query are not going to help us in the near future so i need to delete it
-- lets do it

DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2;
-- now i dont need the row_num1 column so i need to get rid of them
-- lets fix it

ALTER TABLE layoffs_staging2
DROP COLUMN row_num1;
-- so i removed the row_num1 column

SELECT *
FROM layoffs_staging2;
-- This is it, this is my finalized cleaned data




