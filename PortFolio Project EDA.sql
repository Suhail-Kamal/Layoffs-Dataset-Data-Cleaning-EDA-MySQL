-- SQL Project - Exploratory Data Analysis

SELECT *
FROM layoffs_staging2;

-- i most been looking at the total_laid_off column, bcz the percentage_laid off column is not super helpful bcz i dont have an another column saying the total no of employees companies are having

SELECT MAX(total_laid_off),MAX(percentage_laid_off)
FROM layoffs_staging2;

-- its found out be max 12000 employees have been laid off at one go and the max percentage laid off is 1 means 100% of the company employees were laid off
-- lets have a look at those companies where percentage_laid_off = 1 

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;
-- its found to be Amazon,Google,Meta are the top 3 companies having the highest total laid off of employees from the year 2020 to early of year 2023

SELECT MIN(`date`),MAX(`date`)
FROM layoffs_staging2;
-- the min date is 2020-03-11 which is right when the covid-19 break out is started and the max date is 2023-03-06
-- lets look at what industry hit the most during this time  or having the most layoffs
SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;
-- Consumer,Retail industry hit hard bcz due to corona virus and shops are shutting down
-- Manufacturing,Fin-Tech,Aerospace are least affected industries during that time means having least layoffs
SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;
-- united states is the country where most employees had lost their jobs and second is India

-- lets look how much laid off was happened  by date
SELECT `date`, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY `date`
ORDER BY 1 DESC;
-- this result shows the reported total_laid_off as per recent date 
--  Lets do the result as per the year

SELECT YEAR (`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR (`date`)
ORDER BY 1 DESC;
-- its found that in the year 2022 having the highest laid off of employees than the years 2023,2021,2020 but the year 2023 data we got is upto 3 months and it is the second highest laid off of employees
--  so it will be way highest in the year 2023

-- lets have a look at the stage of the company

SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY  stage
ORDER BY 2 DESC;
-- the stage which is Post_IPO(companies like amazon,google,large companies) having the highest laid off

-- 
SELECT company, AVG(percentage_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;
-- i dont think i really want to involve in the percentage laid off too much bcz i dont have the hard percentage of employees in the companies
-- so lets stick to total laid off

-- lets look at the progression of layoff 
-- So lets do the rolling sum of layoffs until the very end of the layoffs

SELECT SUBSTRING(`date`,6,2) AS `MONTH`, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY `MONTH` ;
-- the month from this result dont show us the year its the month from the years 2020,21,22,23 and sum of the total laid off combined into that particular month
-- so lets modify the query to include the year as well 

SELECT SUBSTRING(`date`,1,7) AS `MONTH`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH` 
ORDER BY 1 ASC
;
-- now i want the result from this query and i want to do the rolling sum of it
-- so lets do it

WITH Rolling_Total AS 
(
SELECT SUBSTRING(`date`,1,7) AS `MONTH`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH` 
ORDER BY 1 ASC
)
SELECT `MONTH`,total_off
, SUM(total_off) OVER(ORDER BY `MONTH`) AS rolling_total
FROM Rolling_Total;
-- i got the rolling sum over each month starting from 2020-03 till 2023-03

-- now lets look at the company to know  how much they laying off per year

SELECT company,YEAR(`date`),SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company,YEAR(`date`)
ORDER BY company ASC;
-- lets use this result and rank which year the companies had laid off the most

SELECT company,YEAR(`date`),SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company,YEAR(`date`)
ORDER BY 3 DESC;

WITH Company_Year(company, years,total_laid_off) AS
(
SELECT company,YEAR(`date`),SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company,YEAR(`date`)
),Company_Year_Rank AS
(SELECT *, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year
WHERE years IS NOT NULL
)
SELECT *
FROM Company_Year_Rank
WHERE Ranking <=5
;
-- After running this query i will get the below results
-- In the year 2020 i will get the ranking of 5 companies based upon highest layoffs and i will also get the total number of laid off as well
-- similarly for the year 2021,2022 and 2023


-- End of the EDA project

