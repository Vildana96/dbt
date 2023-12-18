with final as (
SELECT *,  
(y_final_final-cost)/y_final_final AS sales_margin_percent_final_final
FROM(
SELECT f.*, g.y_final_new_unique, g.flag, CASE
WHEN y_final_new_unique IS NOT NULL AND Art_Number_6 IN ('921007', '921004') AND flag='change' THEN ROUND(y_final_new_unique, 2)
ELSE ROUND(y_final_corrected*1.1, 2)
END AS y_final_final
FROM (
  SELECT f.*, cost/NULLIF(1-sales_margin_percent_final_corrected, 0) AS y_final_corrected, 
  FROM(
  SELECT *, CASE 
  WHEN type_new='6' AND (sales_margin_percent_final<=0 OR sales_margin_percent_final>0.6) THEN 0.2
  ELSE sales_margin_percent_final
  END sales_margin_percent_final_corrected
  FROM {{ref('netto_predicted_final')}}
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
        FROM {{ref('netto_predicted_final')}}
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






