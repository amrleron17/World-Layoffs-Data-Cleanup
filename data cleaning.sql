-- Data Cleaning

SELECT *
FROM layoffs;

-- exploratory data analysis - will clean all data and then dive into it to find trends and patterns and all other things

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT *
FROM layoffs_staging;


-- 1. REMOVE DUPLICATES

SELECT *,
ROW_NUMBER () OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,
`date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;


WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER () OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,
`date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- TO CHECK IF THERE IS REALLY A DUPLICATE:
SELECT *
FROM layoffs_staging
WHERE company = 'Casper';


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
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER () OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,
`date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;


DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;
-- this is to check if duplicates were deleted

SELECT *
FROM layoffs_staging2;


-- 2. STANDARDIZE DATA 
	-- if there are issues with the data w/ spellings or things like that we will standardize it to where its all the same as it should

Select *
FROM layoffs_staging2;

-- standardize company
            
SELECT DISTINCT company
FROM layoffs_staging2;           


SELECT company, TRIM(company)               
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = trim(company);         

SELECT company
FROM layoffs_staging2;              


-- standardize industry

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY industry;              -- we saw that the problem is in Crypto

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';              

UPDATE layoffs_staging2
SET industry = 'Crypto'                    
WHERE industry LIKE 'Crypto%';


-- standardize date

SELECT `date`,
str_to_date(`date`, '%m/%d/%Y')                    
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y') ;


-- standardize COUNTRY

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY country;                             


SELECT DISTINCT country, TRIM(TRAILING '.' FROM  country)
FROM layoffs_staging2
ORDER BY country;                 


UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM  country);             


-- WILL CHANGE THE DATATYPE OF 'DATA' COLUMN :
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;



-- 3. NULL VALUES or BLANK VALUES
	-- will see if we can populate that if we can
    
SELECT *
FROM layoffs_staging2
WHERE (industry IS NULL OR industry = '') ;             -- to check null/blank values


SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';                              
  
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

  
SELECT t1.industry, t2.industry                  
FROM layoffs_staging2 t1                         
	JOIN layoffs_staging2 t2                     
		ON t1.company = t2.company              
WHERE t1.industry IS NULL                       
AND t2.industry IS NOT NULL;                     
                                              
UPDATE layoffs_staging2 t1   
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    SET t1.industry = t2.industry                
WHERE t1.industry IS NULL 						
AND t2.industry IS NOT NULL;                    
    

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;   


    
-- 4. REMOVE ANY COLUMNS (ROW_NUM)
ALTER TABLE layoffs_staging2							-- ALTER TABLE used to modify, delete, add in table
DROP row_num;


SELECT *
FROM layoffs_staging2;                                 -- CLEANED


