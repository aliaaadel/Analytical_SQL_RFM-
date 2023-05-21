
-- Question 3  a
-- starting from inner queries to the outer 
-- in the inner most calculate the difference between each transaction date and the one after it using range between and subtract one to not calculate the starting day as a consecutive day 
-- as the consecutive day here is the one that had a transaction in the previous day 
-- it results in the starting day has the value of 0 and each consecutive day has the value of 1 mainly it acts out as a flag 
-- sub 2 query
-- it's then dividded into groups which comes in handy later on a new group is created whenever the flag its'0 and created another when it reaches another 0


-- sub3 
-- query here we are adding up all the days values as it =1 for consecutive and 0 otherwise on the partition by customer and group so that to calculate 
-- consecutive days for each customer 

-- outer query 
-- gets the max values of consecutive days for each cutomer 

with consecutive_days as (
  select  sub3.* ,first_value(sub3.running_sum)  OVER (PARTITION BY sub3.CUST_ID,sub3.groups ORDER BY sub3.running_sum desc  )days
  from(
    select sub2.*, sum(date_diff) OVER (PARTITION BY CUST_ID,groups ORDER BY CALENDAR_DT) running_sum
    from (
        select sub.* ,sum(case when date_diff =1 then 0 else 1 end) OVER (PARTITION BY CUST_ID ORDER BY CALENDAR_DT) groups
        from
        ( 
            select CUST_ID, CALENDAR_DT , COUNT (*) OVER (PARTITION BY CUST_ID ORDER BY CALENDAR_DT   
                                                                                   RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW)-1 AS date_diff
            from transactions) sub) sub2)sub3
)
select CUST_ID , max(days) max_consecutive_days
from consecutive_days cte
group by cust_id
order by max(days) desc;


--Question 3 b
-- inner most query calculate the days for each cutomer not neccessarily consecutive and calculates the running total for each customer 
--outer query 
-- first set condition of total <=250 and then get the highest value of days_count by customer_id 
select distinct(cust_id) ,first_value(days_count) over(PARTITION BY CUST_ID ORDER BY days_count desc) max_days_till_250LE
from(
    select cust_id, calendar_dt,amt_le,
            count(*) over(PARTITION BY CUST_ID ORDER BY CALENDAR_DT) days_count,
            sum(amt_le) over(PARTITION BY CUST_ID order by CALENDAR_DT) total
    from transactions) sub
where total <= 250
order by cust_id;
