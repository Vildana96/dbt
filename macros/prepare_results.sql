{% macro prepare_results(i, size) %}
select CONCAT(CustArtCond_CustBoType, ',', Cust_Number, ',', 1, ',', Art_Number, ';', FORMAT('%.2f', y_final_final_new)) 
        from {{ ref('netto_predicted_results') }}
        WHERE (index_column < ({{i}}+1)*{{size}}) AND (index_column >= {{i}}*{{size}})
{% endmacro %}