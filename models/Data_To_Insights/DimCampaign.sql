
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml
    Try changing "table" to "view" below
*/

 {{
    config(
        materialized='incremental',
		unique_key='CampaignID'
    )
}}

SELECT 
    ROW_NUMBER() OVER (ORDER BY CampaignID) AS CampaignID
	,CampaignID as CampaignSourceKey
	,campaignname
	,description 
	,productid
	,channel 
	,to_date(startdate) as startdate
	,to_date(enddate) as enddate
	,isactive
	,cost
	,_FIVETRAN_SYNCED
	,'fivetran' 
FROM 
    DATA_TO_INSIGHTS.RDS_RAW_DATA_DBO.campaignmaster

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  WHERE _FIVETRAN_SYNCED > (select max(MODIFIEDDATE) from  {{ this }})

{% endif %}

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null