-- recommendation_ctr is defined once: the mart must reconcile with the metric's measures
-- (recommendations_clicked / recommendations_shown over fct_recommendations) at the mart grain.
with metric as (
    select
        event_date,
        model_version,
        strategy,
        count(*) as shown,
        sum(case when is_clicked then 1 else 0 end) as clicked,
        sum(case when is_clicked then 1 else 0 end) / count(*) as ctr
    from {{ ref('fct_recommendations') }}
    group by event_date, model_version, strategy
)

select
    metric.event_date,
    metric.model_version,
    metric.strategy,
    rpt.ctr as rpt_ctr,
    metric.ctr as metric_ctr
from metric
full outer join {{ ref('rpt_recommendation_ctr') }} as rpt
    on
        metric.event_date = rpt.event_date
        and metric.model_version is not distinct from rpt.model_version
        and metric.strategy is not distinct from rpt.strategy
where
    rpt.event_date is null
    or metric.event_date is null
    or rpt.shown != metric.shown
    or rpt.clicked != metric.clicked
    or abs(rpt.ctr - metric.ctr) > 1e-9
