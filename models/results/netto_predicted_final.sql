with NettoPredictedNegative AS (
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
      FROM {{ref('netto_predicted')}}
      WHERE sales_margin_percent_proposed <= 0
      ) AS neg
      INNER JOIN {{ref('netto_predicted')}} AS p
      ON p.Art_Number_6=neg.Art_Number_6 
      AND p.bucket=neg.bucket 
      AND p.ArtSal_Price1=neg.ArtSal_Price1
      )
  )
  WHERE label='good'
)
GROUP BY label, bucket, Art_Number_6, ArtSal_Price1
ORDER BY label, bucket, Art_Number_6, ArtSal_Price1)

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
      FROM {{ref('netto_predicted')}} AS p
      LEFT JOIN
      `NettoPredictedNegative` AS neg
      ON p.Art_Number_6=neg.Art_Number_6 
      AND p.bucket=neg.bucket 
      AND p.ArtSal_Price1=neg.ArtSal_Price1
    )
  )
)
)