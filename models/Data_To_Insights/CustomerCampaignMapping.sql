/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml
    Try changing "table" to "view" below
	Updated Customer & Campaign ID's with Dim Keys
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
,camp.campaignid as campaignid
,cust.customerid as customerid
,ddorder.dateid as campaigndateid
,is_success as issuccess
,to_timestamp(ctc._FIVETRAN_SYNCED) as createddate
,'fivetran' as createdby
from data_to_insights_raw.stg_customertocampaign ctc
LEFT OUTER JOIN DATA_TO_INSIGHTS.DATA_TO_INSIGHTS.DIMCUSTOMER cust on cust.CUSTOMERSOURCEKEY=ctc.CUSTOMER_ID
LEFT OUTER JOIN DATA_TO_INSIGHTS.DATA_TO_INSIGHTS.DIMCAMPAIGN camp on camp.CAMPAIGNSOURCEKEY=ctc.CAMPAIGN_ID
LEFT OUTER JOIN DATA_TO_INSIGHTS.DATA_TO_INSIGHTS.DIMDATE ddorder on ddorder.date=to_date(ctc.campaign_date,'mm/dd/yyyy')

{% if is_incremental() %}

-- this filter will only be applied on an incremental run
WHERE ctc._FIVETRAN_SYNCED > (select max(createddate) from  {{ this }})

{% endif %}
