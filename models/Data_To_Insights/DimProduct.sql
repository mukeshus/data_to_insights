/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml
    Try changing "table" to "view" below
*/

{{
    config(
        materialized='incremental',
		unique_key='ProductSourceKey',
		incremental_strategy='merge'
    )
}}
 
 select 
      DIMPRODUCT_SK.NEXTVAL as PRODUCTID ,
	    productid AS ProductSourceKey
	   , productname
	   , segment
	   , subcategory
	   , category
	   , Current_timestamp() AS CreatedDate
	   , 'fivetran' AS CreatedBy
       , _FIVETRAN_SYNCED AS ModifiedDate
       , 'fivetran' as ModifiedBy
    from rds_raw_data_dbo.productmaster

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  WHERE _FIVETRAN_SYNCED > (select max(MODIFIEDDATE) from  {{ this }})

{% endif %}
/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null