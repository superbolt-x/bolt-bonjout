{{ config (
    alias = target.database + '_blended_performance'
)}}

{% set date_granularity_list = ['day', 'week', 'month', 'quarter', 'year'] %}
    
With meta as (
select date, date_granularity, campaign_type_default,
case when campaign_name ~* 'Canada' then 'CA' else 'US' end as region, 
'Meta' as channel, SUM(coalesce(spend,0)) as spend, SUM(coalesce(purchases,0)) as paid_purchase,
SUM(coalesce(revenue,0)) as paid_revenue,
SUM(coalesce(link_clicks,0)) as clicks,
SUM(coalesce(impressions,0)) as impressions,
0 as shopify_purchase,
0 as shopify_first_orders,
0 as shopify_revenue
from {{ source('reporting', 'facebook_ad_performance') }}
group by 1,2,3,4,5),

pinterest as (
select date, date_granularity, campaign_type_default,
case when campaign_name ~* 'Canada' then 'CA' else 'US' end as region, 
'Pinterest' as channel, SUM(coalesce(spend,0)) as spend, SUM(coalesce(purchases,0)) as paid_purchase,
SUM(coalesce(revenue,0)) as paid_revenue,
SUM(coalesce(clicks,0)) as clicks,
SUM(coalesce(impressions,0)) as impressions,
0 as shopify_purchase,
0 as shopify_first_orders,
0 as shopify_revenue
from {{ source('reporting', 'pinterest_ad_group_performance') }}
group by 1,2,3,4,5),
    
google as (select date, date_granularity,campaign_type_default, 
case when campaign_name ~* 'Canada' then 'CA' else 'US' end as region, 
'Google' as channel, 
SUM(coalesce(spend,0)) as spend, 
SUM(coalesce(purchases,0)) as paid_purchase,
SUM(coalesce(revenue,0)) as paid_revenue,
SUM(coalesce(clicks,0)) as clicks,
SUM(coalesce(impressions,0)) as impressions,
0 as shopify_purchase,
0 as shopify_first_orders,
0 as shopify_revenue
from {{ source('reporting', 'googleads_campaign_performance') }}
group by 1,2,3,4,5),

shopify as (select date, date_granularity,
NULL as campaign_type_default,
region,
'Shopify' as channel, 
0 as spend,
0 as paid_purchase,
0 as paid_revenue,
0 as clicks,
0 as impressions,
sum(coalesce(orders,0)) as shopify_purchase,
sum(coalesce(first_orders,0)) as shopify_first_orders,
sum(coalesce(total_sales,0)) as shopify_revenue
from {{ source('reporting', 'shopify_sales_region') }}
group by 1,2,3,4,5
)

select *
from meta 
union 
select *
from pinterest
union 
select *
from google
union
select *
from shopify
