{{
  config(
    materialized = 'incremental',
    unique_key = 'mappingid',
    merge_update_columns = ['campaign_id', 'customer_id']
  )
}}

SELECT 
campaign_id as campaignid,
customer_id as customerid,
to_date(campaign_date,'mm/dd/yyyy') as campaigndateid,
is_success as issuccess,
to_timestamp(_FIVETRAN_SYNCED) as createddate,
'fivetran' as createdby
from data_to_insights_raw.stg_customertocampaing