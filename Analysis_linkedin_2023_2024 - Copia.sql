/* The main objective of this script is to analyze the data from nile’s LinkedIn page from June 2023 to June 2023. All data present here was exported directed from LinkedIn platform. 

 There are 6 tables in this schema
	content_id: Contains data such as likes, reactions, etc. from the company's LinkedIn page in the last year
	content_posts: Contains data for every content posted company's LinkedIn page in the last year
	followers_new: Contains data about new followers of company's LinkedIn page in the last year
	followers_secteur: Contains data about new followers of company's LinkedIn page in the last year by sector
	visits_stats: Contains data about visitors of company's LinkedIn page in the last year
	visits_secteur: Contains data about visitors of company's LinkedIn page in the last year by sector

Now that the data is cleaned, we are able to answer the following questions:
	 1. What kind of post performed the most in the last year?
     2. What time of the year had posts performed the most?
	 3. Which posts had the best and the worst engagement rate?
	 4. The fact that post the link on the comment section really boosted the performance?
	 5. What is the demographic of people who followed and visited the page in the last year? */

-- 1.  What kind of post performed the most in the last year?      
-- Let's join the datasets 'content_posts_staging' and 'content_posts2_staging' using the 'post_id' as a unique key 

SELECT DISTINCT Type_de_contenu
FROM content_posts2_staging;

SELECT *
FROM content_posts_staging AS c1
INNER JOIN content_posts2_staging  AS cs
USING(post_id);

-- Sorting by type of content (type_de_contenu), let's use agregate functions to calculate the general metrics 

SELECT 
    type_de_contenu,
    COUNT(post_id) AS total_posts,
    SUM(impressions) AS total_impressions,
    SUM(clics) AS total_clics,
    ROUND(AVG(ctr), 2) AS avg_ctr,
    SUM(jaime) AS sum_jaime,	
    SUM(comm) AS total_comm,
    SUM(rep) AS total_rep,
    ROUND(AVG(td), 2) AS avg_taux_engag
FROM content_posts_staging AS c1
INNER JOIN content_posts2_staging AS c2
USING(post_id)
GROUP BY type_de_contenu
ORDER BY total_impressions DESC,
	avg_taux_engag DESC,
    total_clics DESC;	
    
-- We see that Inscriptions and replays had the most impressive performance when talking about impressions. But let’s compare by engagement rate

SELECT c2.type_de_contenu, 
	COUNT(c1.post_id) AS total_posts, 
    ROUND(AVG(c1.td), 2) AS avg_taux_engag,
    ROUND((SELECT ROUND(AVG(c1.td), 2) FROM content_posts_staging GROUP BY type_de_contenu) 
    -
    (SELECT ROUND(AVG(td),2) fROM content_posts_staging),2) AS diff
FROM content_posts_staging AS c1
INNER JOIN content_posts2_staging AS c2
USING(post_id)
GROUP BY type_de_contenu
ORDER BY avg_taux_engag DESC;	

-- Now that we have the general metrics, let's explore more each type of content to see which ones had the best performances. 

SELECT type_de_contenu,
	post_id,
    impressions,
    clics, 
    ROUND(ctr, 2) AS ctr, 
    jaime, 
    comm, 
    rep, 
    ROUND(td, 2) AS taux_engag
FROM content_posts_staging AS c1
INNER JOIN content_posts2_staging AS c2
USING(post_id)
WHERE type_de_contenu = 'Inscription'
ORDER BY impressions DESC;
 
 -- Average impressions from inscriptions
 
SELECT type_de_contenu, AVG(impressions)
FROM content_posts_staging AS c1
INNER JOIN content_posts2_staging AS c2
USING(post_id)
WHERE type_de_contenu = 'inscription';
  
-- 2. What time of the year had posts performed the most?

-- First, let's take a look at the organic indicators in content_ind_staging dataset grouping by year

SELECT *
FROM content_ind_staging;

SELECT YEAR(`date`) AS year, 
	SUM(imp_org) AS sum_imp_org,
    SUM(clics_org) AS sum_clics_org,
    SUM(reactions_org) AS sum_reactions_org,
    SUM(comm_org) AS sum_comm_org,
    SUM(rep_org) AS sum_rep_org,
    ROUND(SUM(td_org), 2) AS sum_td_org 
FROM content_ind_staging
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

-- Let's check the number of posts by year to see if we posted more in the last semester of 2023 of in the first semester of 2024. 

SELECT COUNT(post_id) AS num_posts, 
	YEAR(date) AS Year
FROM content_posts_staging
GROUP BY YEAR(date);

-- Now, lets check by month the number of posts by month

SELECT COUNT(post_id) AS num_posts,
       YEAR(date) AS Year,
       MONTH(date) AS Month
FROM content_posts_staging
GROUP BY YEAR(date), MONTH(date)
ORDER BY COUNT(post_id) DESC;

-- Let's check the month with more reactions in general 

SELECT MONTH(date) AS month,
	YEAR(date) AS year,
	SUM(imp_org) AS total_imp_org, 
    SUM(imp_spon) AS total_imp_spon, 
    SUM(imp_tot) AS total_impressions, 
    SUM(imp_unique) AS total_imp_uniques, 
    SUM(clics_org) AS total_clics_org, 
    SUM(reactions_org) AS total_reactions_org, 
    SUM(comm_org) AS total_comm_org, 
    SUM(comm_tot) AS total_comm, 
    SUM(rep_org) AS total_rep_org, 
    SUM(rep_tot) AS total_rep, 
    ROUND(AVG(td_org), 2) as avg_td_org,
    ROUND(AVG(td_tot), 2) as avg_td_tot
FROM content_ind_staging
GROUP BY month, year
ORDER BY total_impressions DESC, total_comm DESC, avg_td_tot DESC;

-- Let's see the months in which we had the best engagement rates (td) 

SELECT MONTH(date) AS month,
	YEAR(date) AS year,
	SUM(imp_org) AS total_imp_org, 
    SUM(imp_spon) AS total_imp_spon, 
    SUM(imp_tot) AS total_impressions, 
    SUM(imp_unique) AS total_imp_uniques, 
    SUM(clics_org) AS total_clics_org, 
    SUM(reactions_org) AS total_reactions_org, 
    SUM(comm_org) AS total_comm_org, 
    SUM(comm_tot) AS total_comm, 
    SUM(rep_org) AS total_rep_org, 
    SUM(rep_tot) AS total_rep, 
    ROUND(AVG(td_org), 2) as avg_td_org,
    ROUND(AVG(td_tot), 2) as avg_td_tot
FROM content_ind_staging
GROUP BY month, year
HAVING avg_td_tot >= 0.1
ORDER BY avg_td_tot DESC;

 -- 3. Which posts had the best and the worst engagement rate?
 
-- Let's work again with the content_posts_staging focusing on the td

SELECT post_id AS post,
	ROUND(td, 2) AS engagement
FROM content_posts_staging
ORDER BY engagement
LIMIT 10;

 
-- 4. The fact that post the link on the comment section really boosted the performance?

SELECT *
FROM content_posts_staging AS c1
INNER JOIN content_posts2_staging AS c2
USING(post_id);	

-- Let's try to use a CASE WHEN statement with agregated functions to see if the posts having links performed better or worse than the average engagement rate


SELECT ROUND(AVG(c1.td), 2)
FROM content_posts_staging AS c1
INNER JOIN content_posts2_staging AS c2
USING(post_id);

-- We know that the avg_td is 0.8, now let's see if the posts having links performed better than that. 

 
SELECT 
    c1.post_id, 
    ROUND(AVG(c1.td), 2) AS avg_td,
    c2.lien,
    CASE 
        WHEN c2.lien = 'Link' AND ROUND(AVG(c1.td), 2) > 0.8 THEN 'Performed better'
        WHEN c2.lien = 'Link' AND ROUND(AVG(c1.td), 2) < 0.8 THEN 'Performed worse'
        WHEN c2.lien = 'Link' AND ROUND(AVG(c1.td), 2) = 0.8 THEN 'Equal'
        WHEN c2.lien = 'No Link' AND ROUND(AVG(c1.td), 2) > 0.8 THEN 'Performed better'
        WHEN c2.lien = 'No Link' AND ROUND(AVG(c1.td), 2) < 0.8 THEN 'Performed worse'
        WHEN c2.lien = 'No Link' AND ROUND(AVG(c1.td), 2) = 0.8 THEN 'Equal'
        ELSE 'No data'
    END AS Performance
FROM 
    content_posts_staging AS c1
INNER JOIN 
    content_posts2_staging AS c2 USING(post_id)
GROUP BY 
    c1.post_id, c2.lien;  

-- Let's create a temporary table to work with this results as temp_performance

CREATE TEMPORARY TABLE IF NOT EXISTS temp_performance3 AS
SELECT 
    c1.post_id, 
    ROUND(AVG(c1.td), 2) AS avg_td,
    c2.lien,
    CASE 
        WHEN c2.lien = 'Link' AND ROUND(AVG(c1.td), 2) > 0.8 THEN 'Performed better'
        WHEN c2.lien = 'Link' AND ROUND(AVG(c1.td), 2) < 0.8 THEN 'Performed worse'
        WHEN c2.lien = 'Link' AND ROUND(AVG(c1.td), 2) = 0.8 THEN 'Equal'
        WHEN c2.lien = 'No Link' AND ROUND(AVG(c1.td), 2) > 0.8 THEN 'Performed better'
        WHEN c2.lien = 'No Link' AND ROUND(AVG(c1.td), 2) < 0.8 THEN 'Performed worse'
        WHEN c2.lien = 'No Link' AND ROUND(AVG(c1.td), 2) = 0.8 THEN 'Equal'
        ELSE 'No data'
    END AS Performance
FROM 
    content_posts_staging AS c1
INNER JOIN 
    content_posts2_staging AS c2 USING(post_id)
GROUP BY 
    c1.post_id, c2.lien;  
    
SELECT COUNT(*) AS total_posts, 
	lien, 
	performance
FROM temp_performance3
GROUP BY lien, performance;

SELECT *
FROM temp_performance3;


-- Now, let's look again at the INNER JOIN between 'content_posts_staging' and 'content_posts2_staging' to see if we can mesure the performance differently than with engagement rate. 

SELECT *
FROM content_posts_staging AS c1
INNER JOIN content_posts2_staging AS c2
USING(post_id);	

-- CREATE TEMPORARY TABLE IF NOT EXISTS temp_performance_imp AS
SELECT COUNT(*) AS total_posts,
	SUM(c1.impressions) AS sum_impressions,
	ROUND(AVG(c1.impressions),2) AS avg_impressions, 
    c2.lien
FROM content_posts_staging AS c1
INNER JOIN content_posts2_staging AS c2
USING(post_id)
GROUP BY c2.lien
ORDER BY sum_impressions DESC, avg_impressions DESC


 SELECT COUNT(*) AS total_posts,
	SUM(c1.impressions) AS sum_impressions,
	ROUND(AVG(c1.impressions),2) AS avg_impressions,
    c2.lien,
    	CASE 
        WHEN lien = 'Link' AND SUM(c1.impressions) > ROUND(AVG(c1.impressions),2) THEN 'Performed better'
        WHEN lien = 'Link' AND SUM(c1.impressions) < ROUND(AVG(c1.impressions),2) THEN 'Performed worse'
        WHEN lien = 'Link' AND SUM(c1.impressions) = ROUND(AVG(c1.impressions),2) THEN 'Equal'
        WHEN lien = 'No Link' AND SUM(c1.impressions) > ROUND(AVG(c1.impressions),2) THEN 'Performed better'
        WHEN lien = 'No Link' AND SUM(c1.impressions) < ROUND(AVG(c1.impressions),2) THEN 'Performed worse'
        WHEN lien = 'No Link' AND SUM(c1.impressions) = ROUND(AVG(c1.impressions),2) THEN 'Equal'
        ELSE 'No data'
    END AS Performance
FROM content_posts_staging AS c1
INNER JOIN content_posts2_staging AS c2
USING(post_id)
GROUP BY c2.lien
ORDER BY sum_impressions DESC, avg_impressions DESC; 

 
-- 5. What is the demographic of people who followed and visited the page in the last year? */

-- To answer that question, we are going to look at the visits_secteur_staging and followers_secteur_staging datasets. Let's join the two datasets to see the result.

SELECT secteur, 
	SUM(num_views) AS sum_views, 
    SUM(followers_tot) AS sum_followers
FROM visits_secteur_staging 
LEFT JOIN followers_secteur_staging
USING(secteur)
GROUP BY secteur
ORDER BY sum_views DESC, sum_followers DESC
LIMIT 10;
 
