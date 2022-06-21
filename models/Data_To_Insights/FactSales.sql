/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml
    Try changing "table" to "view" below
*/

{{
    config(
        materialized='incremental',
		
		merge_update_columns = ['campaignid', 'customerid','ORDERDATEID','ORDERLOCATIONID','PRODUCTID','SHIPDATEID']
    )
}}

SELECT 
	FACTSALES_SK.nextval AS FACTSALESID,
     cust.customerid AS CUSTOMERID 
	, camp.campaignid AS CAMPAIGNID 
	, ddorder.dateid AS ORDERDATEID 
	, dl.locationid AS ORDERLOCATIONID 
	, dp.productid AS PRODUCTID 
	, ddship.dateid AS SHIPDATEID
	, SAL.VALUE_SHIP_MODE AS SHIPMODE
	, SAL.VALUE_DISCOUNT AS DISCOUNT
	, SAL.VALUE_PROFIT AS PROFIT
	, SAL.VALUE_QUANTITY AS QUANTITY
	, SAL.VALUE_SALES AS SALEAMOUNT
	, SAL.VALUE_SHIPPING_COST AS SHIPPINGCOST
	, TO_TIMESTAMP(SAL.VALUE_CREATED_DATE) AS CREATEDDATE 
	, 'fivetran' AS  CREATEDBY 
    , SAL._FIVETRAN_SYNCED AS MODIFIEDDATE
    , 'fivetran' AS  MODIFIEDBY
FROM 
    DATA_TO_INSIGHTS.DATA_TO_INSIGHTS_RAW.STAGE_SALES_DATA SAL
	LEFT OUTER JOIN DATA_TO_INSIGHTS.DATA_TO_INSIGHTS.DIMCUSTOMER cust on cust.CUSTOMERSOURCEKEY=sal.VALUE_CUSTOMER_ID
	LEFT OUTER JOIN DATA_TO_INSIGHTS.DATA_TO_INSIGHTS.DIMCAMPAIGN camp on camp.CAMPAIGNSOURCEKEY=sal.VALUE_CAMPAIGN_ID
	LEFT OUTER JOIN DATA_TO_INSIGHTS.DATA_TO_INSIGHTS.DIMDATE ddorder on ddorder.date=to_date(sal.value_order_date,'mm/dd/yyyy')
	LEFT OUTER JOIN DATA_TO_INSIGHTS.DATA_TO_INSIGHTS.DIMDATE ddship on ddship.date=to_date(sal.value_ship_date,'mm/dd/yyyy')
	LEFT OUTER JOIN DATA_TO_INSIGHTS.DATA_TO_INSIGHTS.DIMLOCATION dl ON dl.region=sal.VALUE_REGION and dl.state=sal.VALUE_STATE 
	and dl.city=sal.VALUE_CITY and dl.country=sal.VALUE_COUNTRY
	LEFT OUTER JOIN DATA_TO_INSIGHTS.DATA_TO_INSIGHTS.DIMPRODUCT dp on dp.productsourcekey=sal.value_product_id 
		and dp.productname=sal.VALUE_PRODUCT_NAME and dp.SEGMENT=sal.VALUE_SEGMENT and dp.subcategory=sal.VALUE_SUB_CATEGORY and dp.category=sal.VALUE_CATEGORY
	

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  WHERE SAL._FIVETRAN_SYNCED > (select max(MODIFIEDDATE) from  {{ this }})

{% endif %}
/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
