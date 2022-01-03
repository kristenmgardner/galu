with subscriptions AS (
SELECT datecreated::date as subscribed_date
     , count(*) as subscribed
from tmc_van.gal_emailsubscriptions 
where committeeid = '78444'
  group by 1
  order by 1
  
  ),
  
unsubscriptions as (
  select dateunsubscribed::date as unsubscribed_date 
    , count(*) as unsubscribed 
from tmc_van.gal_emailsubscriptions 
where committeeid = '78444'
  and dateunsubscribed is not null
  and dateresubscribed is null
  group by 1
  order by 1
  
),


base as (
  
  select subscribed_date
     , subscribed
     , unsubscribed_date 
     , unsubscribed
    , case when subscribed is null then 0 else subscribed end as subscribers
    , case when unsubscribed is null then 0 else unsubscribed end as unsubscribers 
  from subscriptions
  full outer join unsubscriptions on (subscriptions.subscribed_date = unsubscriptions.unsubscribed_date)
  
)

select coalesce(subscribed_date,unsubscribed_date)::date as "date"
  , subscribers as subscribers
  , unsubscribers as unsubscribers
  , sum(subscribers - unsubscribers) over (order by date asc rows between unbounded preceding and current row) as total_list_size
from base
order by "date"
