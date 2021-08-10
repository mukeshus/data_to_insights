/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml
    Try changing "table" to "view" below
*/

{{
    config(
        materialized='incremental',
		unique_key='CUSTOMERID',
		incremental_strategy='merge'
    )
}}

    SELECT 
	ROW_NUMBER() OVER (ORDER BY s.CUSTOMERID) AS CUSTOMERID
    ,s.CUSTOMERID AS CUSTOMERSOURCEKEY 
	,s.LASTNAME ||', '||s.FIRSTNAME AS CUSTOMERNAME 
	,s.AGE AS AGE 
	,s.GENDER   GENDER 
	,S.EMAIL  EMAIL 
	,' ' AS ADDRESS
	, 1 AS ISACTIVE
	,current_timestamp() AS CREATEDDATE 
	,'fivetran' AS  CREATEDBY 
    ,_FIVETRAN_SYNCED AS MODIFIEDDATE
    , 'fivetran' AS  MODIFIEDBY
FROM 
    "DATA_TO_INSIGHTS"."RDS_RAW_DATA_DBO"."CUSTOMERMASTER" "S"

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  WHERE _FIVETRAN_SYNCED > (select max(MODIFIEDDATE) from  {{ this }})

{% endif %}
/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null