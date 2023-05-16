with RevenueBucketsMedians AS (
SELECT label, bucket, Art_Number_6, ArtSal_Price1,
AVG(sales_margin_percent_median_bucket) AS sales_margin_percent_median_bucket,
AVG(y_median_bucket) AS y_median_bucket,
AVG(y_median_group) AS y_median_group
FROM (
  SELECT *, 
  PERCENTILE_CONT(sales_margin_percent, 0.5) OVER(partition by label, bucket, Art_Number_6, CAST(ArtSal_Price1 AS STRING)) AS sales_margin_percent_median_bucket,
  PERCENTILE_CONT(y, 0.5) OVER(partition by label, bucket, Art_Number_6, CAST(ArtSal_Price1 AS STRING)) AS y_median_bucket,
  PERCENTILE_CONT(y, 0.5) OVER(partition by label, Art_Number_6, CAST(ArtSal_Price1 AS STRING)) AS y_median_group,
  FROM {{ref('netto_historical')}}
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
    FROM {{ref('netto_historical')}} 
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
      {{ref('netto_labels')}} AS n
    LEFT JOIN (
      SELECT
        DISTINCT Cust_Number,
        Art_Number,
        revenue_artikel,
        sales_margin_percent_median_customer
      FROM
        {{ref('netto_historical')}}
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

NettoPredictedFinal as (
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

final as (
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
  FROM NettoPredictedFinal
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
        FROM NettoPredictedFinal
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
),

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




