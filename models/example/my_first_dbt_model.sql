
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='view') }}

select count(*) AS id from `main-beanbag-366508.dbt_vbakarevic.M01Artikel`

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
