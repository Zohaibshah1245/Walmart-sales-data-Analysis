select * from walmart;

create table walmart_data
like walmart;

insert walmart_data
select *
from walmart;
-- i created clone table as walmart because i will not work on orignal dataset
select  distinct * from walmart_data;


select *,
row_number() over(partition by invoice_id ,Branch,category,unit_price,quantity,'date','time',payment_method,rating,profit_margin) as row_num
from walmart_data;


with duplicate_cte as (
select *,
row_number() over(partition by invoice_id ,Branch,category,unit_price,quantity,'date','time',payment_method,rating,profit_margin) as row_num
from walmart_data
)
select * from duplicate_cte
where row_num>1;


select * from walmart_data
where invoice_id=9950
;

-- i made another table walmart_data2 and in this i import whole data of table walmart_data
-- to insert a new column row_num to delete duplicates 
CREATE TABLE `walmart_data1` (
  `invoice_id` int DEFAULT NULL,
  `Branch` text,
  `City` text,
  `category` text,
  `unit_price` text,
  `quantity` int DEFAULT NULL,
  `date` text,
  `time` text,
  `payment_method` text,
  `rating` double DEFAULT NULL,
  `profit_margin` double DEFAULT NULL,
   row_num int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;



select * from walmart_data1;

insert into walmart_data1
select *,
row_number() over(partition by invoice_id ,Branch,category,unit_price,quantity,'date','time',payment_method,rating,profit_margin) as row_num
from walmart_data;

delete from walmart_data1
where row_num >1;


select * from 
walmart_data1
where row_num=1;

-- deleted all duplicates in our datasets


select * from walmart_data1;

update walmart_data1
set unit_price=
replace(unit_price, '$', '');

-- here i remove $ sign from unit_price coloumn,the purpose of removing $ sign is to convert this column data type

alter table walmart_data1
modify unit_price
float;

alter table walmart_data1
modify date
date;

alter table walmart_data1
modify time
time;

alter table walmart_data1
rename column date to dates;

alter table walmart_data1
rename column time to timing;

-- i change the data types of date and time coloumn and also rename date and time coloumn name

select * from walmart_data1;

select unit_price,quantity,concat(unit_price, '*' , quantity)
from walmart_data1;
 
 select unit_price * quantity as total
 from walmart_data1;

alter table walmart_data1
add column total_price float;

update walmart_data1
set total_price = quantity * unit_price;




-- QUESTION NO1 FIND DIFFRENT PAYMENT METHOD,NO OF TRANSACTION AND NO OF QUANTITY SOLD
select * from walmart_data1;


select payment_method ,count(*)as num_of_payment,sum(quantity) AS Num_of_quantity
from walmart_data1
group by payment_method
order by num_of_payment desc;



-- QUESTION NO2: IDENTIFY THE HIGHEST-RATED CATEGORY IN EACH BRANCH,DISPLAYING THE BRANCH CATEGORY,AVG_RATING 

select branch from walmart_data1
order by branch asc;

 
SELECT Branch,category,avg(rating) as avg_rating,
rank()over(partition by Branch order by avg(rating) desc)as Ranking
from walmart_data1
group by Branch,category;

select * from 
(
SELECT Branch,category,avg(rating) as avg_rating,
rank()over(partition by Branch order by avg(rating) desc)as Ranking
from walmart_data1
group by Branch,category
) as ranked_data
where Ranking=1;


-- QUESTION NO 3:
-- Identify the busiest day for each branch based on the number of transitions

select  Branch,category,
   count(*) as no_of_transaction,
   dayname(dates)as day_name,
   rank() over(partition by Branch order by count(*) asc)as ranking
from walmart_data1
group by Branch,category,day_name
;

select * from walmart_data1;

 with busy_day as(
select  Branch,category,
   count(*) as no_of_transaction,
   dayname(dates)as day_name,
   rank()over(partition by Branch order by count(*) desc)as ranking
from walmart_data1
group by Branch,category,day_name

 )
 select * from busy_day
 where ranking >=1
;

 
-- QUESTION NO 4
-- calculate the total quantity  of item sold per payment method,list payment method and total_quantity?  

select * from walmart_data1;

select payment_method,count(*)as no_payment
,sum(quantity)
from walmart_data1
group by payment_method;

-- Question no 5
-- determine the average minimum and maximum rating of category for each city,
-- list the city avg rating, minimum rating and maximum rating

select * from walmart_data1;

select 
city,category,min(rating) as minimum_Rating,
max(rating) as maximum_Rating,
avg(rating) as Average_Rating
from walmart_data1
group by city,category
order by city asc;

-- QUESTION NO 6;
-- calculate the total profit for each category by considering the total profit as 
-- (unit_price * quantity * profit_margin)
-- list category and total_profit,ordered from highest to lowest profit

select * from walmart_data1;

select category,
round(sum(total_price),2) as total_revenue,
round(sum(total_price* profit_margin),2)as profit
from walmart_data1
group by category
order by profit desc;


-- QUESTION NO 7
-- DETERMINE THE MOST COMMON PAYMENT METHOD FOR EACH BRANCH, DISPLAYING BRANCH AND THE PREFRENCE_PAYMENT METHOD ?

SELECT 
  Branch,
  payment_method,
  count(*) as Total_transaction,
  rank() over(partition by Branch order by count(*) desc)as ranking
  from walmart_data1
  group by Branch,payment_method;

  with cte1 as (
   
SELECT 
  Branch,
  payment_method,
  count(*) as Total_transaction,
  rank() over(partition by Branch order by count(*) desc)as ranking
  from walmart_data1
  group by Branch,payment_method
  )
  select * from cte1
  where ranking=1;
  
  -- QUESTION NO 8
  -- CATEGORIZE the sales into 3 group morning,afternoon,evening
  -- find out each of the shift and num of invoices
  
  
  select * from walmart_data1;
select Branch,
 case
     when extract(hour from(timing)) < 12 then 'Morning'
     when extract(hour from(timing)) between 12 and 17 then 'Afternoon'
     else 'Evening'
 end day_time,  
 count(*) as no_of_invoice
 from walmart_data1
 group by Branch,day_time
 order by Branch asc;
 
 -- QUESTION NO 9
 -- IDENTIFY 5 branch with highest decrease ratio in the revenue compare to the last year
 -- (current year 2023 and last year 2022)
 
select * from walmart_data1;
with revenue_2022 as 
(
 select 
 Branch,sum(total_price) as Revenue
 from walmart_data1
 where extract(year from (dates))=2022
 group by Branch
 order by Branch
 ),
 revenue_2023
 as
 (
 select 
 Branch,sum(total_price) as Revenue
 from walmart_data1
 where extract(year from (dates))=2023
 group by Branch
 order by Branch
 )
 select ls.Branch,
 ls.revenue as last_year_revenue,
 cs.revenue as current_year_revenue,
 round((ls.revenue-cs.revenue)/100,2) as revenue_decrease_ratio
 from revenue_2022 as ls
 join 
 revenue_2023 as cs
 on ls.Branch=cs.Branch
 where ls.revenue>cs.revenue
 order by revenue_decrease_ratio desc
 limit 5;
 
 
 