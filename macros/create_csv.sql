{% macro create_csv_i(i, size) %}
    {{ config(
  post_hook = "EXPORT DATA OPTIONS(
    uri='gs://ofrex_data_lake_main-beanbag-366508/dbt/*.csv',
    format='CSV',
    header=false,
    overwrite=true,
    field_delimiter=';') AS {{prepare_results()}} 
         ") }}
select CONCAT(CustArtCond_CustBoType, ',', Cust_Number, ',', 1, ',', Art_Number, ';', FORMAT('%.2f', y_final_final_new)) 
        from {{ ref('netto_predicted_results') }}
        WHERE (index_column < ({{i}}+1)*{{size}}) AND (index_column >= {{i}}*{{size}})
{% endmacro %}