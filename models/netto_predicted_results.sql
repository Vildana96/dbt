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
ORDER BY Cust_Number, Art_Number_6, ArtSal_Price1),

NettopreiseLabel AS (
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
)),

NettoHistorical AS (
SELECT g.* except(Kundennummer), h.*
FROM (
  SELECT *, CASE
  WHEN Cust_CustCondDefBoId='' THEN Cust_Number 
  ELSE CAST(client AS STRING)
  END AS Kundennummer
  FROM NettopreiseLabel) AS g
LEFT JOIN (
  SELECT Kundennummer, Artikelnummer, DokDatum, Menge, KalkEsp, DBCHF, DBCHFMIS, Umsatz, UmsatzMIs, Betrag, PosVP1
  FROM `main-beanbag-366508.dbt_vbakarevic.View_M01_REGS_VDOKITEM_YEAR_TODAY` 
  WHERE DokDatum < '2022-01-01') AS h
ON CAST(h.Kundennummer AS STRING)= g.Kundennummer AND h.Artikelnummer = g.Art_Number),

RevenueBuckets AS (
SELECT *, CASE 
WHEN revenue <= 0 THEN 1
WHEN revenue <= 10000 THEN RANGE_BUCKET(CAST(revenue AS INT64), GENERATE_ARRAY(0, 10000, 1000))
WHEN revenue <= 50000 THEN RANGE_BUCKET(CAST(revenue AS INT64), GENERATE_ARRAY(10000, 50000, 5000)) + 10
ELSE RANGE_BUCKET(CAST(revenue AS INT64), GENERATE_ARRAY(50001, 1000000, 50000)) + 10 + 8
end
AS bucket
FROM (
  SELECT Kundennummer, SUM(UmsatzMIs) AS revenue
  FROM `main-beanbag-366508.dbt_vbakarevic.View_M01_REGS_VDOKITEM_YEAR_TODAY` AS h
  WHERE h.DokDatum >= '2021-01-01' AND h.DokDatum < '2022-01-01'
  GROUP BY Kundennummer
)
ORDER BY revenue DESC),

NettoHistoricalPredicted AS (
SELECT
  h.CustArtCond_CustBoType,
  h.Cust_Number,
  h.Cust_CustCondDefBoId,
  h.Kundennummer,
  h.Art_Number,
  h.Art_Number_6,
  h.ArtSal_Price1,
  h.ArtSal_CalcCCP,
  h.y,
  h.median_y,
  h.max_y,
  h.label,
  h.cost,
  h.margin,
  h.y_old,
  h.sales_margin_percent,
  h.sales_margin_percent_median,
  cnt.revenue_artikel,
  r.revenue,
  (CASE
      WHEN (r.bucket IS NULL) THEN -1
    ELSE
    r.bucket
  END
    ) AS bucket,
  (1 + h.sales_margin_percent_median) * h.cost AS y_proposed,
  m.sales_margin_percent_median_customer
FROM (
  SELECT
    *,
    PERCENTILE_CONT(h.sales_margin_percent, 0.5) OVER(PARTITION BY h.Cust_Number, h.Art_Number_6) AS sales_margin_percent_median,
  FROM (
    SELECT
      *,
      (h.DBCHFMIS/NULLIF(h.Menge, 0))/NULLIF(y_old, 0) AS sales_margin_percent
    FROM (
      SELECT
        *,
        ROUND(h.DBCHFMIS/NULLIF(h.Menge, 0) + h.KalkEsp, 2) AS y_old
      FROM
        NettoHistorical AS h ) AS h ) AS h ) AS h
LEFT JOIN (
  SELECT
    Kundennummer,
    Artikelnummer,
    SUM(UmsatzMIs) AS revenue_artikel
  FROM
    NettoHistorical
  WHERE
    DokDatum >= '2021-01-01'
    AND DokDatum < '2022-01-01'
  GROUP BY
    Kundennummer,
    Artikelnummer ) AS cnt
ON
  h.Art_Number = cnt.Artikelnummer
  AND h.Kundennummer = cnt.Kundennummer
LEFT JOIN
  `RevenueBuckets` AS r
ON
  h.Kundennummer = r.Kundennummer
LEFT JOIN (
  SELECT
    Kundennummer,
    AVG(sales_margin_percent_median_customer) AS sales_margin_percent_median_customer
  FROM (
    SELECT
      Kundennummer,
      PERCENTILE_CONT(sales_margin_percent, 0.5) OVER(PARTITION BY Kundennummer) AS sales_margin_percent_median_customer
    FROM (
      SELECT
        CAST(Kundennummer AS STRING) AS Kundennummer,
        (DBCHFMIS/NULLIF(Menge, 0))/NULLIF(y_old, 0) AS sales_margin_percent
      FROM (
        SELECT
          *,
          ROUND(h.DBCHFMIS/NULLIF(h.Menge, 0) + h.KalkEsp, 2) AS y_old
        FROM
          `main-beanbag-366508.dbt_vbakarevic.View_M01_REGS_VDOKITEM_YEAR_TODAY` AS h
        WHERE
          h.DokDatum >= '2021-01-01'
          AND h.DokDatum < '2022-01-01' ) ))
  GROUP BY
    Kundennummer ) AS m
ON CAST(h.Kundennummer AS STRING)= m.Kundennummer),

RevenueBucketsMedians AS (
SELECT label, bucket, Art_Number_6, ArtSal_Price1,
AVG(sales_margin_percent_median_bucket) AS sales_margin_percent_median_bucket,
AVG(y_median_bucket) AS y_median_bucket,
AVG(y_median_group) AS y_median_group
FROM (
  SELECT *, 
  PERCENTILE_CONT(sales_margin_percent, 0.5) OVER(partition by label, bucket, Art_Number_6, CAST(ArtSal_Price1 AS STRING)) AS sales_margin_percent_median_bucket,
  PERCENTILE_CONT(y, 0.5) OVER(partition by label, bucket, Art_Number_6, CAST(ArtSal_Price1 AS STRING)) AS y_median_bucket,
  PERCENTILE_CONT(y, 0.5) OVER(partition by label, Art_Number_6, CAST(ArtSal_Price1 AS STRING)) AS y_median_group,
  FROM `NettoHistoricalPredicted`
)
GROUP BY label, bucket, Art_Number_6, ArtSal_Price1),

NettoGroupsPredicted AS (
SELECT g.*, CASE
WHEN sales_margin_percent_median IS NOT NULL THEN sales_margin_percent_median
WHEN sales_margin_percent_median_bucket IS NOT NULL AND bucket!=-1 THEN sales_margin_percent_median_bucket
ELSE NULL
END AS sales_margin_percent_median_final
FROM (
  SELECT g.*, r.sales_margin_percent_median_bucket, r.y_median_bucket, r.y_median_group
  FROM (
    SELECT Cust_Number, Art_Number_6, ArtSal_Price1,
    AVG(sales_margin_percent_median) AS sales_margin_percent_median,
    AVG(bucket) AS bucket,
    ANY_VALUE(label) AS label,
    AVG(median_y) AS median_y,
    AVG(max_y) AS max_y
    FROM `NettoHistoricalPredicted` 
    WHERE label='bad' OR (label='good' AND margin < 0) OR Art_Number_6 IN ('921007', '921004')
    GROUP BY Cust_Number, Art_Number_6, ArtSal_Price1
  )AS g
  LEFT JOIN (
    SELECT *
    FROM `RevenueBucketsMedians` 
    WHERE label='good'
  )AS r
  ON g.bucket = r.bucket AND g.Art_Number_6=r.Art_Number_6 AND g.ArtSal_Price1 = r.ArtSal_Price1
) AS g),

NettoPredicted AS (
SELECT
  *,
  (y_proposed-cost)/y_proposed AS sales_margin_percent_proposed
FROM (
  SELECT
    *,
    CASE
      WHEN sales_margin_percent_median_final IS NOT NULL THEN (1 + sales_margin_percent_median_final) * cost
      WHEN y_median_bucket IS NOT NULL
    AND bucket!=-1 THEN y_median_bucket
      WHEN y_median_group IS NOT NULL THEN y_median_group
    ELSE
    median_y
  END
    AS y_proposed,
    CASE
      WHEN sales_margin_percent_median IS NOT NULL THEN '1'
      WHEN sales_margin_percent_median_bucket IS NOT NULL
    AND bucket!=-1 THEN '2'
      WHEN y_median_bucket IS NOT NULL AND bucket!=-1 THEN '3'
      WHEN y_median_group IS NOT NULL THEN '4'
    ELSE
    '5'
  END
    AS type
  FROM (
    SELECT
      n.CustArtCond_CustBoType,
      n.Cust_CustCondDefBoId,
      n.Cust_Number,
      n.Art_Number,
      n.Art_BoName,
      n.Art_Number_6,
      n.cnt_articles,
      n.ArtSal_Price1,
      n.cost,
      n.y,
      n.margin,
      n.label,
      r.revenue_artikel,
      r.sales_margin_percent_median_customer,
      p.bucket,
      p.sales_margin_percent_median,
      p.sales_margin_percent_median_bucket,
      p.sales_margin_percent_median_final,
      p.y_median_bucket,
      p.y_median_group,
      p.median_y,
      p.max_y
    FROM
      `NettopreiseLabel`AS n
    LEFT JOIN (
      SELECT
        DISTINCT Cust_Number,
        Art_Number,
        revenue_artikel,
        sales_margin_percent_median_customer
      FROM
        `NettoHistoricalPredicted`
      WHERE
        revenue_artikel!=0 ) AS r
    ON
      n.Art_Number=r.Art_Number
      AND n.Cust_Number=r.Cust_Number 
    INNER JOIN
      `NettoGroupsPredicted` AS p
    ON
      n.Cust_Number=p.Cust_Number
      AND n.Art_Number_6=p.Art_Number_6
      AND n.ArtSal_Price1=p.ArtSal_Price1) )),

NettoPredictedNegative AS (
SELECT label, bucket, Art_Number_6, ArtSal_Price1,
AVG(sales_margin_percent_median_bucket) AS sales_margin_percent_median_bucket_negative, 
AVG(y_median_bucket) AS y_median_bucket_negative,
AVG(y_median_group) AS y_median_group_negative
FROM (
  SELECT  *, 
  PERCENTILE_CONT(sales_margin_percent, 0.5) OVER(partition by label, bucket, Art_Number_6, CAST(ArtSal_Price1 AS STRING)) AS sales_margin_percent_median_bucket,
  PERCENTILE_CONT(y, 0.5) OVER(partition by label, bucket, Art_Number_6, CAST(ArtSal_Price1 AS STRING)) AS y_median_bucket,
  PERCENTILE_CONT(y, 0.5) OVER(partition by label, Art_Number_6, CAST(ArtSal_Price1 AS STRING)) AS y_median_group
  FROM (
    SELECT Cust_Number, p.Art_Number_6, p.ArtSal_Price1, 
    label, CAST(p.bucket AS INT64) AS bucket,
    sales_margin_percent_proposed AS sales_margin_percent, y_proposed AS y
    FROM(
      (SELECT DISTINCT Art_Number_6, bucket, ArtSal_Price1,
      FROM `NettoPredicted`
      WHERE sales_margin_percent_proposed <= 0
      ) AS neg
      INNER JOIN `NettoPredicted` AS p
      ON p.Art_Number_6=neg.Art_Number_6 
      AND p.bucket=neg.bucket 
      AND p.ArtSal_Price1=neg.ArtSal_Price1
      )
  )
  WHERE label='good'
)
GROUP BY label, bucket, Art_Number_6, ArtSal_Price1
ORDER BY label, bucket, Art_Number_6, ArtSal_Price1),

NettoPredictedFinal AS (
SELECT *
FROM (
SELECT *, (y_final_new-cost)/y_final_new AS sales_margin_percent_final
FROM (
  SELECT *, CASE
  WHEN y_final-cost <= 0 AND median_y-cost > 0 THEN median_y
  WHEN y_final-cost <= 0 AND max_y > y_final THEN max_y
  ELSE y_final
  END AS y_final_new,
  CASE 
  WHEN y_final-cost <= 0 THEN '6'
  ELSE type
  END type_new
  FROM(
    SELECT *, CASE
    WHEN sales_margin_percent_proposed <= 0 AND sales_margin_percent_median_bucket_negative > 0 AND bucket!=-1 THEN (1 +    sales_margin_percent_median_bucket_negative) * cost
    WHEN sales_margin_percent_proposed <= 0 AND bucket!=-1 AND y_median_bucket_negative IS NOT NULL THEN y_median_bucket_negative
    WHEN sales_margin_percent_proposed <= 0 AND y_median_group_negative IS NOT NULL THEN y_median_group_negative
    ELSE y_proposed
    END AS y_final
    FROM (
      SELECT p.*, neg.sales_margin_percent_median_bucket_negative, neg.y_median_bucket_negative, neg.y_median_group_negative
      FROM `NettoPredicted` AS p
      LEFT JOIN
      `NettoPredictedNegative` AS neg
      ON p.Art_Number_6=neg.Art_Number_6 
      AND p.bucket=neg.bucket 
      AND p.ArtSal_Price1=neg.ArtSal_Price1
    )
  )
)
)),

final AS (
SELECT *,  
(y_final_final-cost)/y_final_final AS sales_margin_percent_final_final
FROM(
SELECT f.*, g.y_final_new_unique, g.flag, CASE
WHEN y_final_new_unique IS NOT NULL AND Art_Number_6 IN ('921007', '921004') AND flag='change' THEN ROUND(y_final_new_unique, 2)
ELSE ROUND(y_final_corrected*1.1, 2)
END AS y_final_final
FROM (
  SELECT f.*, cost/(1-sales_margin_percent_final_corrected) AS y_final_corrected, 
  FROM(
  SELECT *, CASE 
  WHEN type_new='6' AND (sales_margin_percent_final<=0 OR sales_margin_percent_final>0.6) THEN 0.2
  ELSE sales_margin_percent_final
  END sales_margin_percent_final_corrected
  FROM `NettoPredictedFinal`
  ) AS f
) AS f
LEFT JOIN (
  SELECT Cust_Number, CASE
  WHEN MIN(y_final_new_good) IS NOT NULL THEN MIN(y_final_new_good)
  ELSE MIN(y_final_new)*1.1 
  END AS y_final_new_unique,
  CASE
  WHEN ANY_VALUE(label)='good' AND COUNT(DISTINCT label)=1 THEN 'no change'
  ELSE 'change'
  END AS flag
  FROM (
  SELECT *, CASE
      WHEN label='good' AND margin > 0 THEN y
      ELSE NULL
      END y_final_new_good
      FROM (
        SELECT Cust_Number, Art_Number_6, 
        AVG(y_final_new) AS y_final_new, AVG(y) AS y,
        ANY_VALUE(label) AS label, AVG(margin) AS margin
        FROM `NettoPredictedFinal`
        WHERE Art_Number_6 IN ('921007', '921004') 
        GROUP BY Cust_Number, Art_Number_6
      ) 
 )
 GROUP BY Cust_Number
) AS g
ON f.Cust_Number=g.Cust_Number
)
WHERE (flag='change' AND Art_Number_6 IN ('921007', '921004')) OR
(Art_Number_6 NOT IN ('921007', '921004'))
)

SELECT ROW_NUMBER() OVER() AS index_column, *
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY Cust_Number, Art_Number) AS row_distinct
FROM (
SELECT *, 
(y_final_final_new-cost)/y_final_final_new AS sales_margin_percent_final_final_new
FROM (
  SELECT f.*, CASE
  WHEN y_final_corrected_unique IS NOT NULL AND Art_Number_6 IN ('921007', '921004') AND flag='change' THEN ROUND(y_final_corrected_unique, 2)
  ELSE y_final_final
  END AS y_final_final_new
  FROM final AS f
  LEFT JOIN (
    SELECT Cust_Number, MIN(y_final_corrected) AS y_final_corrected_unique
    FROM final
    WHERE sales_margin_percent_final_final <= 0 
    GROUP BY Cust_Number
  ) AS g
  ON f.Cust_Number=g.Cust_Number
)
)
)
WHERE row_distinct = 1


