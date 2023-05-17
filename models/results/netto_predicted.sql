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
) AS g)

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
      AND n.ArtSal_Price1=p.ArtSal_Price1) )