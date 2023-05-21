-- started by craeting the RFM values first 
with RFM as(
    select distinct(customer_id)  , count(distinct(invoice)) over(partition by customer_id) frequency,
     round(max(invoicedate)  over () - max(invoicedate) over( partition by customer_id)) recency,
     sum(price*quantity ) over( partition by customer_id) monetary
    from tableretail 
order by customer_id
)
-- then creating the system by calculating each score 
, sys as(
select customer_id, frequency,recency,monetary,r_score, round((f_score+m_score)/2) fm_score
from (
    select cte.* , ntile(5) over(order by frequency) f_score ,
    ntile(5) over(order by recency desc) r_score,
    ntile(5) over(order by monetary) m_score
    from RFM cte)
order by customer_id
)

-- segementation of the customers based on the scores 
SELECT 
    s.customer_id,
    s.r_score,
    s.fm_score,
    CASE CONCAT(r_score, fm_score)
        WHEN '55' THEN 'Champions'
        WHEN '54' THEN 'Champions'
        WHEN '45' THEN 'Champions'
        WHEN '52' THEN 'Potential Loyalists'
        WHEN '42' THEN 'Potential Loyalists'
        WHEN '33' THEN 'Potential Loyalists'
        WHEN '43' THEN 'Potential Loyalists'
        WHEN '53' THEN 'Loyal Customers'
        WHEN '44' THEN 'Loyal Customers'
        WHEN '35' THEN 'Loyal Customers'
        WHEN '34' THEN 'Loyal Customers'
        WHEN '51' THEN 'Recent Customers'
        WHEN '41' THEN 'Promising'
        WHEN '31' THEN 'Promising'
        WHEN '32' THEN 'Customers Needing Attention'
        WHEN '23' THEN 'Customers Needing Attention'
        WHEN '22' THEN 'Customers Needing Attention'
        WHEN '25' THEN 'At Risk'
        WHEN '24' THEN 'At Risk'
        WHEN '13' THEN 'At Risk'
        WHEN '15' THEN 'Can''t Lose Them'
        WHEN '14' THEN 'Can''t Lose Them'
        WHEN '12' THEN 'Hibernating'
        WHEN '11' THEN 'Lost'
        ELSE 'other'
    END AS customer_group
    
    from sys s;




--ALTER SESSION SET NLS_DATE_FORMAT = 'MM/DD/YYYY HH24:MI';
