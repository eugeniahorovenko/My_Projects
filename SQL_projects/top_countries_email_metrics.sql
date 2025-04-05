-- CTE для збору метрик електронної пошти з групуванням за датою, країною та іншими параметрами

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
   `DA.account_session` acs ON es.id_account = acs.account_id
   JOIN
   `DA.account` ac ON acs.account_id = ac.id
   JOIN
  `DA.session` as s ON acs.ga_session_id = s.ga_session_id
   JOIN
  `DA.session_params` as sp ON acs.ga_session_id = sp.ga_session_id
 LEFT JOIN
     `DA.email_open` eo ON es.id_message = eo.id_message
 LEFT JOIN
     `DA.email_visit` ev ON es.id_message = ev.id_message
  GROUP BY 1,2,3,4,5
  ),

-- CTE для збору інформації про облікові записи з групуванням за тими ж параметрами

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
 `DA.account` ac ON acs.account_id = ac.id
 JOIN
   `DA.session` s ON acs.ga_session_id = s.ga_session_id
  JOIN
   `DA.session_params` sp ON acs.ga_session_id = sp.ga_session_id
   GROUP BY
   s.date,
   sp.country,
   send_interval,
   is_verified,
   is_unsubscribed
   ),


-- Об'єднання таблиць email_metrics та account_information за допомогою UNION ALL

   union_table AS (
 SELECT
   date,
   country,
   send_interval,
   is_verified,
   is_unsubscribed,
   0 AS account_cnt,   -- Нульове значення для метрики облікових записів
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
   0 AS sent_msg,    -- Нульові значення для метрик повідомлень
   0 AS open_msg,
   0 AS visit_msg,
 FROM
   account_information 
   ),


-- Агрегація даних після об'єднання для отримання сумарних метрик

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
1,2,3,4,5
,


 -- Обчислення загальних показників за країнами за допомогою віконних функцій

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


-- Ранжування країн за загальною кількістю облікових записів та відправлених повідомлень

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
from select_table_2
)


-- Фінальний запит, що вибирає тільки топ-10 країн за кожним з критеріїв

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
FROM final_select
WHERE rank_total_country_account_cnt <=10 OR rank_total_country_sent_cnt <=10
