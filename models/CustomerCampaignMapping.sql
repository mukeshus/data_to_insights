{{ config(materialized='table') }}

MERGE INTO customercampaignmapping tgt using(
SELECT 
campaign_id,
customer_id,
to_date(campaign_date,'mm/dd/yyyy') as campaign_date,
is_success,
to_timestamp(_FIVETRAN_SYNCED) as modifieddate
from data_to_insights_raw.stg_customertocampaing
) src on src.campaign_id=tgt.campaignid and src.customer_id=tgt.customerid
when matched then
update set tgt.campaignid=src.campaign_id,tgt.customerid=src.customer_id,tgt.campaigndateid=src.campaign_date,
tgt.issuccess=src.is_success,tgt.createddate=src.modifieddate
when not matched then
insert (campaignid, customerid, campaigndateid, issuccess, createddate, createdby)
values (src.campaign_id,src.customer_id,src.campaign_date,src.is_success,src.modifieddate,'fivetran')


