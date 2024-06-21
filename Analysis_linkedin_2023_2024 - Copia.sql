/* The main objective of this script is to analyze the data from nile’s LinkedIn page from June 2023 to June 2023. All data present here was exported directed from LinkedIn platform. 

 There are 6 tables in this schema
	content_id: Contains data such as likes, reactions, etc. from the company's LinkedIn page in the last year
	content_posts: Contains data for every content posted on the company's LinkedIn page in the last year
	followers_new: Contains data about new followers of the company's LinkedIn page in the last year
	followers_secteur: Contains data about new followers the company's LinkedIn page in the last year by sector
	visits_stats: Contains data about visitors of the company's LinkedIn page in the last year
	visits_secteur: Contains data about visitors of the company's LinkedIn page in the last year by sector

Now that the data is cleaned, we are able to answer the following questions:
	 1. What kind of post performed the most in the last year?
     2. What time of the year had posts performed the most?
	 3. Which posts had the best and the worst engagement rate?
	 4. The fact that post the link on the comment section really boosted the performance?
	 5. What is the demographic of people who followed and visited the page in the last year? */

-- 1.      
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
  
