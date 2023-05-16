with NettoHistorical AS (
SELECT g.* except(Kundennummer), h.*
FROM (
  SELECT *, CASE
  WHEN Cust_CustCondDefBoId='' THEN Cust_Number 
  ELSE CAST(client AS STRING)
  END AS Kundennummer
  FROM {{ref('netto_labels')}}) AS g
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
ORDER BY revenue DESC)

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
ON CAST(h.Kundennummer AS STRING)= m.Kundennummer