/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml
    Try changing "table" to "view" below
*/
{{
  config(
    materialized = 'incremental',
	unique_key = 'mappingid',
    merge_update_columns = ['campaignid', 'customerid']
  )
}}

SELECT
ROW_NUMBER() OVER (ORDER BY campaign_id,customer_id) AS mappingid 
,campaign_id as campaignid
,customer_id as customerid
,to_date(campaign_date,'mm/dd/yyyy') as campaigndateid
,is_success as issuccess
,_FIVETRAN_SYNCED as createddate
,'fivetran' as createdby
from data_to_insights_raw.stg_customertocampaing

{% if is_incremental() %}

-- this filter will only be applied on an incremental run
WHERE _FIVETRAN_SYNCED > (select max(createddate) from  {{ this }})

{% endif %}