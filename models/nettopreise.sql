with ArtikelFiltered as (
SELECT * FROM (SELECT a.Art_Number, a.Art_BoId, a.Art_ArtMisC1No 
  FROM main-beanbag-366508.dbt.M01Artikel AS a 
		INNER JOIN main-beanbag-366508.dbt.ArtikelActive AS ap 
      ON a.Art_Number = ap.ArtNr 
        WHERE NOT a.Art_Number LIKE '%M%' AND ap.Passiv=0
) AS af
INNER JOIN main-beanbag-366508.dbt.M01ArtSal AS sal
ON sal.ArtSal_BoId = af.Art_BoId),

KundeUnique AS (
SELECT DISTINCT cond.CustArtCond_CustBoType, cond.CustArtCond_CustBoId, kunde1.Cust_Number, 
'' AS Cust_CustCondDefBoId, NULL AS client
FROM (
  SELECT *
  FROM `main-beanbag-366508.dbt.M01CustArtCond`
  WHERE CustArtCond_CustBoType='1'AND CustArtCond_CustBoId >= 10000
) AS cond
INNER JOIN `main-beanbag-366508.dbt.M01Kunde` AS kunde1
ON cond.CustArtCond_CustBoId=kunde1.Cust_Number
UNION ALL
SELECT DISTINCT cond.CustArtCond_CustBoType, cond.CustArtCond_CustBoId, NULL,
kunde2.Cust_CustCondDefBoId, kunde2.Cust_Number AS client
FROM (
  SELECT *
  FROM `main-beanbag-366508.dbt.M01CustArtCond`
  WHERE CustArtCond_CustBoType='2'
) AS cond
INNER JOIN `main-beanbag-366508.dbt.M01Kunde` AS kunde2
ON CAST(cond.CustArtCond_CustBoId AS STRING)=kunde2.Cust_CustCondDefBoId)

SELECT *, CAST(CASE
  WHEN Cust_Number IS NOT NULL THEN CAST(Cust_Number AS STRING)
  ELSE CONCAT(CustArtCond_CustBoId, client)
  END AS STRING) AS customer,
  CASE 
  WHEN SalCondPrice_SpRebatePerc < 0 THEN true
  ELSE false
  END AS mark_up,
  CASE 
  WHEN SalCondPrice_SpRebatePerc < 0 THEN ArtSal_CalcCCP*(1-SalCondPrice_SpRebatePerc/100)
  WHEN SalCondPrice_Price1=0 THEN ArtSal_Price1-ArtSal_Price1*SalCondPrice_SpRebatePerc/100
  ELSE SalCondPrice_Price1
  END AS y
FROM (
  SELECT fb.*, cond1.SalCondPrice_Price1, cond1.SalCondPrice_SpRebatePerc,
  cond1.CustArtCond_ArtBoType
  FROM
  (
    SELECT * FROM ArtikelFiltered as af
    CROSS JOIN KundeUnique as kunde
  ) AS fb
  LEFT JOIN (
    SELECT *
    FROM (
      SELECT *
      FROM main-beanbag-366508.dbt.M01CustArtCond
      WHERE CustArtCond_CustBoType='2'
    ) AS cond 
    INNER JOIN (
      SELECT *
      FROM main-beanbag-366508.dbt.M01SalCondPrice 
      WHERE NOT (SalCondPrice_Price1=0 AND SalCondPrice_SpRebatePerc=0)
    ) AS sal
    ON cond.CustArtCond_ActualNormalPriceBoId = sal.SalCondPrice_BoId
  ) AS cond1
  ON fb.Cust_CustCondDefBoId = CAST(cond1.CustArtCond_CustBoId AS STRING) AND
  fb.Art_Number = cond1.CustArtCond_ArtBoId
  WHERE (cond1.CustArtCond_ArtBoType = '1' AND cond1.CustArtCond_NormalPriceTypeCd = '2')
  UNION ALL
  SELECT fb.*, cond2.SalCondPrice_Price1, cond2.SalCondPrice_SpRebatePerc,
  cond2.CustArtCond_ArtBoType
  FROM
  (
    SELECT * FROM ArtikelFiltered as af
    CROSS JOIN KundeUnique as kunde
  ) AS fb
  LEFT JOIN (
    SELECT *
    FROM (
      SELECT *
      FROM main-beanbag-366508.dbt.M01CustArtCond
      WHERE CustArtCond_CustBoType='1' AND CustArtCond_CustBoId >= 10000
    ) AS cond
    INNER JOIN (
      SELECT *
      FROM main-beanbag-366508.dbt.M01SalCondPrice 
      WHERE NOT (SalCondPrice_Price1=0 AND SalCondPrice_SpRebatePerc=0)
    ) AS sal
    ON cond.CustArtCond_ActualNormalPriceBoId = sal.SalCondPrice_BoId) AS cond2
  ON fb.CustArtCond_CustBoId = cond2.CustArtCond_CustBoId AND
  fb.Art_Number = cond2.CustArtCond_ArtBoId
  WHERE (cond2.CustArtCond_ArtBoType = '1' AND cond2.CustArtCond_NormalPriceTypeCd = '2')
)
WHERE Cust_CustCondDefBoId NOT IN (
  SELECT DISTINCT CAST(Nr_ AS STRING)
  FROM `main-beanbag-366508.dbt.CustVordef`
)