
-- Question 1
-- at what time the transactions are the highest
select sub.*
from(
SELECT distinct(TO_CHAR(invoicedate, 'HH24')) as hour_only, count(*) over(partition by TO_CHAR(invoicedate, 'HH24')) no_transactions
FROM tableRetail)sub
order by no_transactions desc

-- top 10 selling products 

select  STOCKCODE, sales ,product_rank
from (
  select  STOCKCODE ,sum(price * quantity)  sales,
         rank() over (order by sum(price * quantity) desc) product_rank
  from tableRetail
  group by STOCKCODE
)
where product_rank <= 10;


-- change in sales from prev month 
with sales_change as(
    select sub.*, lead(sales,1) over(order by sub.date_month desc ) prev_month_sales from(
        select to_char(invoicedate, 'YYYY-MM')  date_month ,
               sum(price * quantity) over(partition by to_char(invoicedate, 'YYYY-MM')) sales
        from tableRetail)sub
    group by sub.date_month, sub.sales
)
select cte.* , round(((sales-prev_month_sales)/prev_month_sales)*100,2)  change
from sales_change cte ;

-- what are the sales for weekdays and weekend per month
select  sub.*
from( select
      sum(case when to_char(invoicedate, 'D') in (1, 7) then price * quantity else 0 end ) over(partition by to_char(invoicedate, 'YYYY-MM')) weekend_sales,
      sum(case when to_char(invoicedate, 'D') not in (1, 7) then price * quantity else 0 end ) over(partition by to_char(invoicedate, 'YYYY-MM')) weekday_sales,
      to_char(invoicedate, 'YYYY-MM') date_month
    from tableRetail) sub
group by sub.date_month,sub.weekend_sales,sub.weekday_sales
order by  sub.weekday_sales desc 



