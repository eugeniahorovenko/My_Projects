-- CTE для збору даних про метрики електронної пошти та користувачів

WITH email_metrics AS (
SELECT
    acs.ga_session_id,
    a.is_unsubscribed,
    es.id_message AS id_message_sent,
    ev.id_message AS id_message_visit,
    eo.id_message AS id_message_open,
 FROM
   `DA.email_sent` es
   JOIN
   `DA.account_session` acs ON es.id_account = acs.account_id
   JOIN
  `DA.account` as a ON acs.account_id = a.id
 LEFT JOIN
     `DA.email_open` eo ON es.id_message = eo.id_message
 LEFT JOIN
     `DA.email_visit` ev ON es.id_message = ev.id_message
)

-- Фінальний запит для аналізу метрик електронної пошти за операційними системами

   SELECT
    sp.operating_system,
    COUNT(DISTINCT id_message_open) AS open_msg,
    COUNT(DISTINCT id_message_sent) AS sent_msg,
    COUNT(DISTINCT id_message_visit) AS vist_msg,
    COUNT(DISTINCT id_message_open) / COUNT(DISTINCT id_message_sent) * 100 AS open_rate,
    COUNT(DISTINCT id_message_visit) / COUNT(DISTINCT id_message_sent) * 100 AS click_rate,
    COUNT(DISTINCT id_message_visit) / COUNT(DISTINCT id_message_open) * 100 AS ctor,
FROM 
email_metrics as em
JOIN
 `DA.session_params` as sp ON em.ga_session_id = sp.ga_session_id
 WHERE 
 is_unsubscribed = 0
 GROUP BY 1
