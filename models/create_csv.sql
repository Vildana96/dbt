{{ config(
  post_hook = "EXPORT DATA OPTIONS(
    uri='gs://ofrex_data_lake_main-beanbag-366508/dbt/*.csv',
    format='CSV',
    header=false,
    overwrite=true,
    field_delimiter=';') AS (
        select CONCAT(CustArtCond_CustBoType, ',', Cust_Number, ',', 1, ',', Art_Number, ';', FORMAT('%.2f', y_final_final_new)) AS result
        from {{ ref('netto_predicted_results') }}) 
         ") }}
select CONCAT(CustArtCond_CustBoType, ',', Cust_Number, ',', 1, ',', Art_Number, ';', FORMAT('%.2f', y_final_final_new)) AS result
        from {{ ref('netto_predicted_results') }}