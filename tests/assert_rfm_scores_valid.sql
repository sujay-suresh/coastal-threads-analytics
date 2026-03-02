-- Assert that all RFM scores are within valid range (1-5)
select
    customer_id,
    recency_score,
    frequency_score,
    monetary_score

from {{ ref('int_customers_rfm_scored') }}
where recency_score not in (1, 2, 3, 4, 5)
   or frequency_score not in (1, 2, 3, 4, 5)
   or monetary_score not in (1, 2, 3, 4, 5)
