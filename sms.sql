WITH outgoing AS (
select date(date_trunc('week', date(sent_at))) as Week
   , count(distinct id) as outgoing
from tmc_strive.gal_outgoing_messages
where week >= date('2021-01-01')
  group by 1
  
),

incoming AS (
 select date(date_trunc('week', date(sent_at))) as Week
   , count(distinct id) as incoming
from tmc_strive.gal_incoming_messages
where week >= date('2021-01-01')
  group by 1
  
)
  
  
SELECT incoming.week
     , sum(outgoing) as total_outgoing
     , sum(incoming) as total_incoming
     from incoming
     left join outgoing using (week)
group by 1 order by 1
