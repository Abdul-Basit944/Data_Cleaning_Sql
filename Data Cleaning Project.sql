use world_layoffs;
select * from layoffs;
-- 1. Remove Duplicates
-- 2. Standardize Data
-- 3. Null Values
-- 4. Remove unneccessary data

Create Table layoffs_Staging
Like layoffs;

INSERT layoffs_Staging
Select * From layoffs;

select * from layoffs_Staging;


WITH Duplicate_cte AS
(
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY company, location, industry, total_laid_off, `date`, stage, country, funds_raised_millions
           ORDER BY company
         ) AS row_num
  FROM layoffs_staging
)
SELECT *
FROM Duplicate_cte
where row_num > 1;

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
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_staging2
SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY company, location, industry, total_laid_off, `date`, stage, country, funds_raised_millions
           ORDER BY company
         ) AS row_num
  FROM layoffs_staging;
  
SET SQL_SAFE_UPDATES = 0;

DELETE FROM layoffs_Staging2
WHERE row_num > 1;

select * from layoffs_Staging2;

-- Standardizing Data 

select * from layoffs_Staging2;

select company , trim(company)
from layoffs_Staging2;

update layoffs_Staging2
set company = trim(company);

Update layoffs_Staging2
set industry = 'Crypto'
where industry Like 'Crypto%';

select * from layoffs_Staging2;

select distinct country 
from layoffs_Staging2;

select distinct country , trim(TRAILING '.' from country)
from layoffs_Staging2
order by 1;

Update layoffs_Staging2 
Set country = trim(TRAILING '.' from country)
where country like 'United States%';

select distinct country from layoffs_Staging2;

select `date`
from layoffs_Staging2;

select `date` ,
str_to_date(`date` , '%m/%d/%Y')
from layoffs_Staging2;

Update layoffs_Staging2 
SET `date` = str_to_date(`date` , '%m/%d/%Y');

select * from layoffs_Staging2;

Alter Table layoffs_Staging2
Modify Column `date` DATE;

select * from layoffs_Staging2;

select company,industry
from  layoffs_Staging2
where company = 'Airbnb';

Update layoffs_Staging2
SET industry = Null
where industry = '';


Select *
from layoffs_Staging2 c1
join layoffs_Staging2 c2
   on c1.company = c2.company
where c1.industry is Null
And c2.industry is Not Null;

Update layoffs_Staging2 c1
Join layoffs_Staging2 c2
   on c1.company = c2.company
set c1.industry = c2.industry
where c1.industry is null
and c2.industry is not null;

select company,industry from layoffs_Staging2
where company='Airbnb';

select * from layoffs_Staging2;

Delete
from layoffs_Staging2
where total_laid_off is null
and percentage_laid_off is null;

Alter table layoffs_Staging2
Drop Column row_num;

select * from layoffs_Staging2;

