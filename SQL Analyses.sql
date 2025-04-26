--q1
with tracks_count as(
select playlistid ,
	   count(trackid) as count_of_tracks
from dwh.dim_playlist dp 
group by playlistid
--in this subqery we calculated the count of tracks per playlist
)
select *,
(select avg(count_of_tracks) from tracks_count) as avg_tracks_count -- we got the avg of tracks in playlist
from tracks_count
where count_of_tracks= (select  min(count_of_tracks) from tracks_count) 
	  or
	  count_of_tracks= (select  max(count_of_tracks) from tracks_count)
	  --we got the playlists with the minimum and maximum amount of tracks
group by playlistid, count_of_tracks


--q2
with track_sales as (
select trackid, sum(quantity) as sales_n
from dwh.fact_invoiceline
group by trackid
),
track_grups as (
select trackid, sales_n, case when sales_n =0 then '0'
when sales_n between 1 and 5 then '1-5'
when sales_n between 6 and 10 then '6-10'
else '>10' end as sales_group
from track_sales
order by sales_n
)
select sales_group, count (trackid) as track_count
from track_grups
group by sales_group

--First get the sales count by track, then divide in four groups by the count of sales. Finishing with the count of tracks in each group



--q3 a
(
select dc.country 
	  , sum(fi.total) revenue_per_country
	  ,row_number () over(order by sum(fi.total) ) as rating
from dwh.fact_invoice fi 
left join dwh.dim_customer dc 
on fi.customerid =dc.customerid 
group by dc.country 
order by revenue_per_country
limit 5
)
union all
(
select dc.country 
	  , sum(fi.total) revenue_per_country
	  ,row_number () over(order by sum(fi.total) desc) as rating
from dwh.fact_invoice fi 
left join dwh.dim_customer dc 
on fi.customerid =dc.customerid 
group by dc.country 
order by revenue_per_country desc
limit 5
) 
--we found the top 5 countries with the lowest revenue, and union it with the top 5 countries with the highest revenue
--we added another row which rating the numbers of revenue in both queries. 


--q3b
with max_min_5 as (
    -- Table with the top 5 and bottom 5 countries by revenue, from q3a
    (select dc.country 
           , sum(fi.total) as revenue_per_country
           , row_number() over (order by sum(fi.total) desc) as rating
    from dwh.fact_invoice fi 
    left join dwh.dim_customer dc 
      on fi.customerid = dc.customerid 
    group by dc.country
    order by revenue_per_country desc
    limit 5)
    union all
    (select dc.country 
           , sum(fi.total) as revenue_per_country
           , row_number() over (order by sum(fi.total)) as rating
    from dwh.fact_invoice fi 
    left join dwh.dim_customer dc 
      on fi.customerid = dc.customerid 
    group by dc.country
    order by revenue_per_country
    limit 5)
)
-- Calculate revenue by genre for each country from max_min_5
select 
    dc.country, 
    dt.genre_name, 
    -- Calculate the percentage of genre sales out of total sales for each country
    cast( (sum(fil.unitprice) / total_revenue.total_revenue) * 100 as decimal(10,2)) as genre_sales_percentage,
    -- Calculate the rank for each genre within the same country, ordered by percentage sales
    row_number() over (partition by dc.country order by (cast( (sum(fil.unitprice) / total_revenue.total_revenue) * 100 as decimal(10,2))) desc) as genre_sales_rank
from dwh.fact_invoice fi
left join dwh.fact_invoiceline fil
    on fi.invoiceid = fil.invoiceid
left join dwh.dim_track dt
    on fil.trackid = dt.trackid
left join dwh.dim_customer dc
    on fi.customerid = dc.customerid
-- Subquery to get total revenue per country
left join (
    select dc.country, sum(fi.total) as total_revenue
    from dwh.fact_invoice fi
    left join dwh.dim_customer dc
    on fi.customerid = dc.customerid
    group by dc.country
) total_revenue
    on dc.country = total_revenue.country
where dc.country in (select country from max_min_5)  -- get only the countries from max_min_5
group by dc.country, dt.genre_name, total_revenue.total_revenue
order by dc.country, genre_sales_percentage desc




--q4
select case when count_customers = 1 then 'other'
      	  else country
  		  end as country, 
		  --checking if there is only one customer in the country. if there is, it changes the country name to 'other'
	      sum(count_customers) as count_customers,  --summing all the customers in the countries that has one customer
	     -- sum(total_of_sales) as total_of_sales,
          cast(cast(sum(total_of_sales) as decimal(10, 2)) / sum(count_customers)as decimal(10,2)) as avg_sales_per_customer_$,
	     --  sum(sum_per_country) as sum_per_country,
	      cast(cast(sum(sum_per_country) as decimal(10, 2)) / sum(count_customers) as decimal(10,2)) as avg_revenue_per_customer_$
from (
    select 
        dc.country,
        count(distinct dc.customerid) as count_customers,
        count(*) as total_of_sales, --count of the sales per country
        sum(fi.total) as sum_per_country --summing the revenue per country
    from dwh.dim_customer dc
    join dwh.fact_invoice fi on dc.customerid = fi.customerid
    group by dc.country
) as country_stats
group by case when count_customers = 1 then 'other'
       	 else country
    	 end 
		 --the grouping is done by the case so that all the 'other' countries will be group by together too
order by country


--q5
with emp_per_year as(
select 
    de.employeeid,
    de.firstname,
    de.lastname,
    de.years_hired ,
    extract (year from fi.invoicedate) as "year",
    count(distinct dc.customerid) as total_customers_handled,
    sum(fi.total) as revenue_per_employee
from  dwh.fact_invoice fi 
left join  dwh.dim_customer dc on  fi.customerid = dc.customerid 
left join dwh.dim_employee de on dc.supportrepid = de.employeeid 
group by 
    de.employeeid, 
    de.firstname, 
    de.lastname, 
    de.years_hired,
    extract (year from fi.invoicedate)
order by 
    de.years_hired desc
-- in this subquery we calculated the count of customers and revenue per employee per year
),
previous_revenue as(
select *,
       lag(revenue_per_employee,1,revenue_per_employee) over(partition by employeeid order by  "year") as prev_revenue
from emp_per_year
)
select employeeid,
       firstname,
       lastname,
       years_hired as tenure,
       "year",
       total_customers_handled,
       revenue_per_employee,
       concat(cast((revenue_per_employee- prev_revenue)/ prev_revenue * 100 as decimal(10,2)), '%') as revenue_growth
from previous_revenue


--extra question- 
with ranked_employees as (
    select
        de.employeeid,
        de.firstname,
        de.lastname,
        extract(year from fi.invoicedate) as year,
        sum(fil.unitprice) as revenue_per_employee_$,
        row_number() over (partition by extract(year from fi.invoicedate) order by sum(fil.unitprice) desc) as best_seller_employee
    from dwh.dim_customer dc
    join dwh.dim_employee de on dc.supportrepid = de.employeeid
    join dwh.fact_invoice fi on dc.customerid = fi.customerid
    join dwh.fact_invoiceline fil on fi.invoiceid = fil.invoiceid
    group by de.employeeid, de.firstname, de.lastname, de.is_manager, extract(year from fi.invoicedate)
)
select
    employeeid,
    firstname,
    lastname,
	year,
    revenue_per_employee_$
from ranked_employees
where best_seller_employee = 1
order by year;

--Employee number 4 had the highest sales in 2018,
-- but from 2019 to 2021, employee number 5 exceeded employee number 4's sales.
--in 2022, employee number 4 regained the lead.
-- Over the years, these two employees were the highest sellers in this period