select * from df_orders

-- we extracted data from kaggle
-- then loaded into sql server 
-- then we will do some data cleaning
-- so we did and ETL project

-- find top 10 highest revenue generating products


select top 10 product_id,sum(sale_price) as revenue
from df_orders
group by product_id
order by sum(sale_price) desc



-- top5 highest selling products in each region
with cte as (select Region,product_id,sum(sale_price) as revenue
from df_orders
group by region, product_id)

select * from (
select a.* ,
rank() over (partition by Region order by revenue desc) as rnk 
from cte a)V 
where V.rnk<=5


-- find month over month growth comparison for 2022 and 2023 eg: jan 2022 and jan 2023

with cte as (select year(order_date) as order_year, month(order_date) as order_month,
sum(sale_price) as sales
from df_orders
group by year(order_date) , month(order_date)
)


select order_month ,
round(sum(case when order_year=2022 then sales else 0 end),2) as sales_2022,
round(sum(case when order_year=2023 then sales else 0 end),2) as sales_2023
from cte 
group by  order_month



--for each category which month has highest sales 

with cte as (
select category,format(order_date,'yyyyMM') as order_year_month
, sum(sale_price) as sales 
from df_orders
group by category,format(order_date,'yyyyMM')
--order by category,format(order_date,'yyyyMM')
)


select * from (
select *,
row_number() over(partition by category order by sales desc) as rn
from cte
) a
where rn=1


--which sub category had highest growth by profit in 2023 compare to 2022
with cte as (
select sub_category,year(order_date) as order_year,
sum(sale_price) as sales
from df_orders
group by sub_category,year(order_date)
--order by year(order_date),month(order_date)
	)
, cte2 as (
select sub_category
, sum(case when order_year=2022 then sales else 0 end) as sales_2022
, sum(case when order_year=2023 then sales else 0 end) as sales_2023
from cte 
group by sub_category
)
select top 1 *
,(sales_2023-sales_2022)
from  cte2
order by (sales_2023-sales_2022) desc

