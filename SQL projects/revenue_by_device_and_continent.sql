-- CTE для обчислення показників доходу за континентами, сегментованих за типом пристрою

WITH
 revenue_usd AS(
 SELECT
   sp.continent,
   SUM(p.price) AS revenue,
   SUM(CASE WHEN device = 'mobile' THEN p.price END) AS revenue_from_mobile,
   SUM(CASE WHEN device = 'desktop' THEN p.price END) AS revenue_from_desktop,
 FROM
   `DA.order` o
 JOIN
   `DA.product` p ON o.item_id = p.item_id
 JOIN
   `DA.session_params` sp ON o.ga_session_id = sp.ga_session_id
 GROUP BY
   sp.continent 
   ),

-- CTE для збору метрик облікових записів та сесій за континентами

   acc_information AS (
 SELECT
   sp.continent,
   COUNT(sp.ga_session_id) AS session_cnt,
   COUNT(ac.account_id) AS account_cnt,
   COUNT(case when acc.is_verified > 0 then acc.id end) AS verified_account
 FROM
   `DA.session_params` sp
 LEFT JOIN
   `DA.account_session` ac ON sp.ga_session_id = ac.ga_session_id
 LEFT JOIN
 `DA.account` acc ON acc.id = ac.account_id
 GROUP BY
   sp.continent 
   )

-- Фінальний запит, що об'єднує дані про доходи та облікові записи з розрахунками відсотків

SELECT
   acc_information.continent,
   revenue_usd.revenue,
   revenue_usd.revenue_from_mobile,
   revenue_usd.revenue_from_desktop,
   revenue_usd.revenue / SUM(revenue_usd.revenue) OVER() *100 AS revenue_from_total,
   revenue_usd.revenue_from_mobile / SUM(revenue_usd.revenue) OVER() *100 AS mobile_revenue_from_total,
   revenue_usd.revenue_from_desktop / SUM(revenue_usd.revenue) OVER() *100 AS desktop_revenue_from_total,
   acc_information.session_cnt,
   acc_information.account_cnt,
   acc_information.verified_account,
FROM acc_information
LEFT JOIN revenue_usd ON acc_information.continent = revenue_usd.continent


WITH email_metrics AS (
  SELECT
DATE_ADD(s.date, INTERVAL es.sent_date DAY) AS date,
sp.country,
send_interval,
is_verified,
is_unsubscribed,
COUNT(DISTINCT es.id_message) AS sent_msg,
COUNT(DISTINCT ev.id_message) AS visit_msg,
COUNT(DISTINCT eo.id_message) AS open_msg,
 FROM
   `DA.email_sent` es
   JOIN
   `DA.account_session` acs
 ON
   es.id_account = acs.account_id
   JOIN
   `DA.account` ac
 ON
   acs.account_id = ac.id
   JOIN
  `DA.session` as s
  ON
   acs.ga_session_id = s.ga_session_id
   JOIN
  `DA.session_params` as sp
  ON
   acs.ga_session_id = sp.ga_session_id
 LEFT JOIN
     `DA.email_open` eo
 ON
   es.id_message = eo.id_message
 LEFT JOIN
     `DA.email_visit` ev
 ON
   es.id_message = ev.id_message
  GROUP BY 1,2,3,4,5),


-- таблиця з інформацією про акаунти. Для виводу інформ про дату та країну обєднала з `DA.account`, `DA.session` та  `DA.session_params`




account_information AS (
SELECT
   s.date,
   sp.country,
   send_interval,
   is_verified,
   is_unsubscribed,
   COUNT(DISTINCT acs.account_id) AS account_cnt
 FROM
   `DA.account_session` acs
   JOIN
   `DA.account` ac
 ON
   acs.account_id = ac.id
 JOIN
   `DA.session` s
 ON
   acs.ga_session_id = s.ga_session_id
  JOIN
   `DA.session_params` sp
 ON
acs.ga_session_id = sp.ga_session_id
   GROUP BY
   s.date,
   sp.country,
   send_interval,
   is_verified,
   is_unsubscribed),


-- таблиця, в якій обєдную дані двох попередніх за допомогою UNION


   union_table AS (
 SELECT
   date,
   country,
   send_interval,
   is_verified,
   is_unsubscribed,
   0 AS account_cnt,
    open_msg,
    sent_msg,
    visit_msg
 FROM
   email_metrics
 UNION ALL
 SELECT
   date,
   country,
   send_interval,
   is_verified,
   is_unsubscribed,
   account_cnt,
   0 AS sent_msg,
   0 AS open_msg,
   0 AS visit_msg,
 FROM
   account_information ),


-- таблиця, в якій в якій виводжу дані обєднання після UNION




select_table_1 AS(
 SELECT
   date,
   country,
 send_interval,
 is_verified,
 is_unsubscribed,
 SUM(account_cnt) AS account_cnt,
 SUM(sent_msg) AS sent_msg,
 SUM(open_msg) AS open_msg,
 SUM(visit_msg) AS visit_msg,
FROM
 union_table
GROUP BY
1,2,3,4,5),


 
-- таблиця, в якій за допомогою Window Function рахую total_country_sent_cnt та total_country_account_cnt


select_table_2 AS(
 SELECT
date,
country,
send_interval,
is_verified,
is_unsubscribed,
account_cnt,
sent_msg,
open_msg,
visit_msg,
SUM(account_cnt) over (partition by country) as total_country_account_cnt,
SUM(sent_msg) over (partition by country) as total_country_sent_cnt,
FROM select_table_1
),


-- таблиця, в якій за допомогою Window Function рахую rank_total_country_sent_cnt та rank_total_country_account_cnt


final_select AS(
SELECT
date,
country,
send_interval,
is_verified,
is_unsubscribed,
account_cnt,
sent_msg,
open_msg,
visit_msg,
total_country_account_cnt,
total_country_sent_cnt,
DENSE_RANK() over (ORDER BY total_country_account_cnt DESC) as rank_total_country_account_cnt,
DENSE_RANK() over (ORDER BY total_country_sent_cnt DESC) as rank_total_country_sent_cnt,
from select_table_2)


-- фінальний запит


SELECT
date,
country,
send_interval,
is_verified,
is_unsubscribed,
account_cnt,
sent_msg,
open_msg,
visit_msg,
total_country_account_cnt,
total_country_sent_cnt,
rank_total_country_account_cnt,
rank_total_country_sent_cnt,
from final_select
WHERE rank_total_country_account_cnt <=10 OR rank_total_country_sent_cnt <=10
