{{ config (
    alias = target.database + '_blended_performance'
)}}

{% set date_granularity_list = ['day', 'week', 'month', 'quarter', 'year'] %}
    
With meta as (
select date, date_granularity, 'Meta' as channel, SUM(coalesce(spend,0)) as spend, SUM(coalesce(purchases,0)) as paid_purchase,
SUM(coalesce(revenue,0)) as paid_revenue,
SUM(coalesce(link_clicks,0)) as clicks,
SUM(coalesce(impressions,0)) as impressions,
0 as shopify_purchase,
0 as shopify_first_orders,
0 as shopify_revenue
from {{ source('reporting', 'facebook_ad_performance') }}
group by 1,2),

google as (select date, date_granularity, 'Google' as channel, SUM(coalesce(spend,0)) as spend, SUM(coalesce(purchases,0)) as paid_purchase,
SUM(coalesce(revenue,0)) as paid_revenue,
SUM(coalesce(clicks,0)) as clicks,
SUM(coalesce(impressions,0)) as impressions,
0 as shopify_purchase,
0 as shopify_first_orders,
0 as shopify_revenue
from {{ source('reporting', 'googleads_campaign_performance') }}
group by 1,2),

shopify as (
{%- for date_granularity in date_granularity_list %}
select DATE_TRUNC('{{date_granularity}}' ,date::date) as date, '{{date_granularity}}' as date_granularity, 'Shopify' as channel, 
0 as spend,
0 as paid_purchase,
0 as paid_revenue,
0 as clicks,
0 as impressions,
sum(coalesce(orders,0)) as shopify_purchase,
sum(coalesce(first_orders,0)) as shopify_first_orders,
sum(coalesce(total_sales,0)) as shopify_revenue
from {{ source('reporting', 'shopify_sales_region') }}
group by 1,2
    {% if not loop.last %}UNION ALL
    {% endif %}
{% endfor %}
)

select *
from meta 
union 
select *
from google
union
select *
from shopify
