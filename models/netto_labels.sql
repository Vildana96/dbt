with NettopreiseResearch AS (
SELECT *, 
PERCENTILE_CONT(y, 0.5) OVER(partition by Cust_Number, Art_Number_6, CAST(ArtSal_Price1 AS STRING)) AS median_y,
PERCENTILE_CONT(ArtSal_CalcCCP, 0.5) OVER(partition by Cust_Number, Art_Number_6, CAST(ArtSal_Price1 AS STRING)) AS median_cost,
MAX(y) OVER(partition by Cust_Number, Art_Number_6, CAST(ArtSal_Price1 AS STRING)) AS max_y
FROM (
  SELECT CustArtCond_CustBoType, CASE
  WHEN Cust_Number IS NOT NULL THEN CAST(Cust_Number AS STRING)
  ELSE Cust_CustCondDefBoId
  END AS Cust_Number, 
  Cust_CustCondDefBoId, client,
  art.Art_BoName, 
  neto.Art_Number, SUBSTRING(neto.Art_Number, 1, 6) AS Art_Number_6,
  ArtSal_Price1, ArtSal_CalcCCP, y
  FROM (
    SELECT *
    FROM {{ ref('nettopreise') }}
    WHERE SalCondPrice_Price1!=0 AND SalCondPrice_SpRebatePerc=0
  ) AS neto
  INNER JOIN `main-beanbag-366508.dbt_vbakarevic.M01Artikel` AS art
  ON neto.Art_Number = art.Art_Number
)),

NettopreiseCountY AS (
SELECT Cust_Number, Art_Number_6, ArtSal_Price1,
string_agg(Art_Number, ',') AS Art_Number_List,
COUNT(DISTINCT Art_Number) AS cnt_articles,
COUNT(DISTINCT ArtSal_Price1) AS cnt_vp1,
COUNT(DISTINCT y) AS cnt_y, 
COUNT(DISTINCT ArtSal_CalcCCP) AS cnt_cost,
AVG(median_y) AS median_y,
AVG(median_cost) AS median_cost,
AVG(max_y) AS max_y
FROM NettopreiseResearch
GROUP BY Cust_Number, Art_Number_6, ArtSal_Price1
ORDER BY Cust_Number, Art_Number_6, ArtSal_Price1)

SELECT *, (y-cost)/y AS margin
FROM (
  SELECT *, CASE 
  WHEN cnt_articles > 1  AND cnt_y>1 THEN 'bad'
  WHEN cnt_y=1 OR Art_Number_6 IN ('921007', '921004') THEN 'good' 
  ELSE ''
  END AS label
  FROM (
    SELECT neto.*, cnt.cnt_vp1, cnt.cnt_y, cnt.cnt_articles, 
    cnt.cnt_cost, cnt.median_cost AS cost
    FROM NettopreiseResearch AS neto
    INNER JOIN NettopreiseCountY AS cnt
    ON neto.Art_Number_6=cnt.Art_Number_6 AND cnt.Cust_Number=neto.Cust_Number
    AND neto.ArtSal_Price1 = cnt.ArtSal_Price1
  )
)