select
    event_date,
    model_version,
    strategy,
    count(*) as shown,
    sum(case when is_clicked then 1 else 0 end) as clicked,
    sum(case when is_clicked then 1 else 0 end) / count(*) as ctr,
    sum(case when is_clicked and rank <= 1 then 1 else 0 end)
    / nullif(sum(case when rank <= 1 then 1 else 0 end), 0) as ctr_at_1,
    sum(case when is_clicked and rank <= 2 then 1 else 0 end)
    / nullif(sum(case when rank <= 2 then 1 else 0 end), 0) as ctr_at_2,
    sum(case when is_clicked and rank <= 3 then 1 else 0 end)
    / nullif(sum(case when rank <= 3 then 1 else 0 end), 0) as ctr_at_3
from {{ ref('fct_recommendations') }}
group by event_date, model_version, strategy
