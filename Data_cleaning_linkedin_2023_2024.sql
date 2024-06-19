/* The main objective of this script is to analyze the data from a company’s LinkedIn page from June 2023 to June 2023. All data present here was exported directed from LinkedIn platform. 

 There are 6 tables in this schema
	content_id: Contains data such as likes, reactions, etc. from the company’s LinkedIn page page in the last year
	content_posts: Contains data for every content posted on the company’s LinkedIn page in the last year
	followers_new: Contains data about new followers of the company’s LinkedIn page in the last year
	followers_secteur: Contains data about new followers of the company’s LinkedIn page in the last year by sector
	visits_stats: Contains data about visitors of the company’s LinkedIn page in the last year
	visits_secteur: Contains data about visitors of the company’s LinkedIn page in the last year by sector

After cleaning the data, we expect to be able to answer the following questions:
	 What kind of post performed the most in the last year?
     What time of the had posts performed the most?
	 Which posts had the best and the worst engagement rate?
	 The fact that post the link on the comment section really boosted the performance?
	 What is the demographic of people who followed and visited the page in the last year? */


-- DATA CLEANING 

SELECT *
FROM content_ind;

/* 1. First thing we want to do is create a staging table from each table in our schema. This is the one we will work in and clean the data. 
We want a table with the raw data in case something happens, for instance during the changing of formats. */

-- Staging table for content_ind
CREATE TABLE content_ind_staging
LIKE content_ind;

INSERT content_ind_staging
SELECT *
FROM content_ind;

SELECT *
FROM content_ind_staging;

-- Staging table for content_posts
CREATE TABLE content_posts_staging
LIKE content_posts;

INSERT content_posts_staging
SELECT *
FROM content_posts;

SELECT *
FROM content_posts_staging;

-- Staging table for followers_new
CREATE TABLE followers_new_staging
LIKE followers_new;

INSERT followers_new_staging
SELECT *
FROM followers_new;

SELECT *
FROM followers_new_staging;

-- Staging table for followers_secteur
CREATE TABLE followers_secteur_staging
LIKE followers_secteur;

INSERT followers_secteur_staging
SELECT *
FROM followers_secteur;

SELECT *
FROM followers_secteur_staging;

-- Staging table for visits_stats
CREATE TABLE visits_stats_staging
LIKE visits_stats;

INSERT visits_stats_staging
SELECT *
FROM visits_stats;

SELECT *
FROM visits_stats_staging;

-- Staging table for visits_secteur
CREATE TABLE visits_secteur_staging
LIKE visits_secteur;

INSERT visits_secteur_staging
SELECT *
FROM visits_secteur;

SELECT *
FROM visits_secteur_staging;
 
-- 2. Remove duplicates 

-- First, we are going to see if there's any duplicates

SELECT *
FROM content_ind_staging;

SELECT *,
ROW_NUMBER() OVER (
PARTITION BY date, imp_org, imp_spon, imp_tot, imp_unique, clics_org, clics_spon, clics_tot, reactions_org, reactions_spon, reactions_tot, comm_org, comm_spon, comm_tot,
			rep_org, rep_spon, rep_tot, td_org, td_spon, td_tot) AS row_num
FROM content_ind_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY date, imp_org, imp_spon, imp_tot, imp_unique, clics_org, clics_spon, clics_tot, reactions_org, reactions_spon, reactions_tot, comm_org, comm_spon, comm_tot,
			rep_org, rep_spon, rep_tot, td_org, td_spon, td_tot) AS row_num
FROM content_ind_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

SELECT *
FROM content_ind_staging
WHERE imp_org = 29;

-- There's no duplicates in content_ind_staging
-- Let's look at content_posts_staging

SELECT *
FROM content_posts_staging;

SELECT *,
ROW_NUMBER() OVER (
PARTITION BY post_id, lien, post_type, camp_nom, author, date, date_debut, date_fin, audience, impressions, vues_hors_videos, vues_hors_site, clics, ctr, 
		jaime, comm, rep, suivis, td, contenu_type) AS row_num
FROM content_posts_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY post_id, lien, post_type, camp_nom, author, date, date_debut, date_fin, audience, impressions, vues_hors_videos, vues_hors_site, clics, ctr, 
		jaime, comm, rep, suivis, td, contenu_type) AS row_num
FROM content_posts_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- There's no duplicates in content_posts_staging
-- Let's look at followers_new_staging

SELECT *
FROM followers_new_staging;

SELECT *,
ROW_NUMBER() OVER (
PARTITION BY date, followers_spon, followers_org, followers_tot) AS row_num
FROM followers_new_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY date, followers_spon, followers_org, followers_tot) AS row_num
FROM followers_new_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- There's no duplicates in followers_new_staging
-- Let's look at followers_secteur_staging

SELECT *
FROM followers_secteur_staging;

SELECT *,
ROW_NUMBER() OVER (
PARTITION BY secteur, followers_tot) AS row_num
FROM followers_secteur_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY secteur, followers_tot) AS row_num
FROM followers_secteur_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- There's no duplicates in followers_secteur_staging
-- Let's look at visits_secteur_staging


SELECT *
FROM visits_secteur_staging;

SELECT *,
ROW_NUMBER() OVER (
PARTITION BY secteur, num_views) AS row_num
FROM visits_secteur_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY secteur, num_views) AS row_num
FROM visits_secteur_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- There's no duplicates in visits_secteur_staging
-- Let's look at visits_stats_staging

SELECT *
FROM visits_stats_staging; 

SELECT *,
ROW_NUMBER() OVER (
PARTITION BY date, views_desktop, views_mobile, views_tot, visits_uni_desktop, visits_uni_mobile, visits_uni_tot, views_tot_desktop, views_tot_mobile, views_tot_page) AS row_num
FROM visits_stats_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY date, views_desktop, views_mobile, views_tot, visits_uni_desktop, visits_uni_mobile, visits_uni_tot, views_tot_desktop, views_tot_mobile, views_tot_page) AS row_num
FROM visits_stats_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- There's no duplicates in visits_stats_staging

-- THERE'S NO DUPLICATES IN ANY OF DATASETS

-- 3. Standardize the data

-- Some tables have problems with utf8 encoding. It must be solved. 

UPDATE followers_secteur_staging
SET secteur = CONVERT(BINARY CONVERT(secteur USING latin1) USING utf8);

UPDATE visits_secteur_staging
SET secteur = CONVERT(BINARY CONVERT(secteur USING latin1) USING utf8);

UPDATE content_posts_staging
SET contenu_type = CONVERT(BINARY CONVERT(contenu_type USING latin1) USING utf8);

-- Some datasets contains date in a different format, we must standardize it to ' 

SELECT date
FROM content_ind_staging;

UPDATE nile_link_2024.content_ind_staging
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT date
FROM content_posts_staging;

UPDATE nile_link_2024.content_posts_staging
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT *
FROM followers_new_staging;

UPDATE nile_link_2024.followers_new_staging
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT *
FROM visits_stats_staging;

UPDATE nile_link_2024.visits_stats_staging
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Some datasets have FLOAT columns describe as TEXT because the decimal is marked by ',' and not '.'. It should be changed and after that converted for FLOAT. 

-- Changing data on content_ind_staging to float 
SELECT td_org, td_tot
FROM content_ind_staging;

DESCRIBE content_ind_staging;

UPDATE content_ind_staging
SET td_org = REPLACE(td_org, ',', '.'),
	td_tot = REPLACE(td_tot, ',', '.');
    
ALTER TABLE content_ind_staging
MODIFY td_org FLOAT,
MODIFY td_tot FLOAT;

DESCRIBE content_ind_staging;

-- Changing data on content_posts_staging to float 

SELECT ctr, td
FROM content_posts_staging;

UPDATE content_posts_staging
SET ctr = REPLACE(ctr, ',', '.'),
	td = REPLACE(td, ',', '.');
    
ALTER TABLE content_posts_staging
MODIFY ctr FLOAT,
MODIFY td FLOAT;

DESCRIBE content_posts_staging;

-- 4. Null values or blank values
/*I already know by seaching the database that there's no null values. But I could use ISNULL() function or the IS NULL to detect then in each column as showed below
using visits_secteur_staging*/

SELECT *,
ISNULL(num_views) AS null_values
FROM visits_secteur_staging
ORDER BY null_values;
 
SELECT *
FROM visits_secteur_staging
WHERE secteur IS NULL
	OR num_views IS NULL;


 -- 5. Remove any columns or rows 
 
 -- Finally, we can drop some columns that contains zero or blank values or simply aren't usefull for our analysis
 -- I already know that nile promote just two posts last year, so we could check if there is values different than 0 in all columns finishing by 'spon' of sponsorisé. 
 
 SELECT *
 FROM content_ind_staging
 WHERE imp_spon > 0;
 
 -- We have two records with values in imp_spon columns that are different from 0 so we can't delete this column. Let's check the rest of them. 
 
 SELECT *
 FROM content_ind_staging
 WHERE td_spon > 0;
 
 -- After checkin, we see that the columns clics_spon, reactions_spon, comm_spon, rep_spon, and td_spon contains all 0 values. So they can be deleted. 
 
ALTER TABLE content_ind_staging
DROP COLUMN clics_spon,
DROP COLUMN reactions_spon,
DROP COLUMN comm_spon,
DROP COLUMN rep_spon,
DROP COLUMN td_spon;

 SELECT *
 FROM content_ind_staging;
 
 -- I have a feeling that imp_org and imp_tot columns are the same, as well as td_org and td_tot, clics_org and clics_tot, reactions_org and reactions_tot. Let's check this and if it's true drop one of them. 
 
SELECT COUNT(*) AS equal_count
FROM content_ind_staging
WHERE reactions_org = reactions_tot;

SELECT imp_org, imp_tot
FROM content_ind_staging
WHERE imp_org <> imp_tot;

SELECT td_org, td_tot
FROM content_ind_staging
WHERE td_org <> td_tot;

/*Two values are different in 'imp_org' and 'imp_tot', also in 'td_org' and 'td_tot' so they can't be deleted. 
However, there's no different values in  'clics_org' and 'clics_tot' and 'reactions_org' and 'reactions_tot'. So one of them can be deleted*/

ALTER TABLE content_ind_staging
DROP COLUMN clics_tot,
DROP COLUMN reactions_tot;

SELECT *
FROM content_ind_staging;

-- The 'content_ind_staging' dataset is now ready for analysis. Let's check the other datasets. 

-- We don't need the 'lien' column, as well 'post_type', 'author' and 'audience' columns. Let's drop them. 

SELECT *
FROM content_posts_staging;

ALTER TABLE content_posts_staging
DROP COLUMN lien,
DROP COLUMN post_type,
DROP COLUMN author,
DROP COLUMN audience;

-- Let's check if the columns 'camp_nom', 'date_debut', 'date_fin', 'vues_hors_videos', 'vues_hors_site', 'suivis', and 'contenu_type' are all blanks. 

SELECT 
    SUM(camp_nom = '' OR camp_nom IS NULL) AS camp_nom_blanks,
    SUM(date_debut = '' OR date_debut IS NULL) AS date_debut_blanks,
    SUM(date_fin = '' OR date_fin IS NULL) AS date_fin_blanks,
    SUM(vues_hors_videos = '' OR vues_hors_videos IS NULL) AS vues_hors_videos_blanks,
    SUM(vues_hors_site = '' OR vues_hors_site IS NULL) AS vues_hors_site_blanks,
    SUM(suivis = '' OR suivis IS NULL) AS suivis_blanks,
    SUM(contenu_type = '' OR contenu_type IS NULL) AS contenu_type_blanks
FROM content_posts_staging;

-- Only the columns vues_hors_videos and contenu_types have some data in them. So we keep them and drop the others. 

ALTER TABLE content_posts_staging
DROP COLUMN camp_nom,
DROP COLUMN date_debut,
DROP COLUMN date_fin,
DROP COLUMN vues_hors_site,
DROP COLUMN suivis;

-- The datase 'content_posts_staging' is ready for analysis. 

-- Let's check the followers_new_staging dataset

SELECT *
FROM followers_new_staging;

SELECT *
FROM followers_new_staging
WHERE followers_spon > 0;

-- There's one record in followers_spon column, so it can't be deleted. 

-- Let's check the followers_secteur_staging dataset

SELECT *
FROM followers_secteur_staging;

-- Nothing to delete in followers_secteur_staging

-- Let's check the visits_secteur_staging dataset

SELECT *
FROM visits_secteur_staging;

-- Nothing to delete in visits_secteur_staging

-- Let's check visits_stats_staging

SELECT *
FROM visits_stats_staging;

-- Nothing to delete in visits_stats_staging. 

-- All datasets are ready to the analysis 
 
 
