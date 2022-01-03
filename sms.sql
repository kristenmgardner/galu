--usage summary --

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


--list size summary --
with list_growth as (
  
  select 
      created_at::date as opted_in_date
    , count(*) as opted_in
  from tmc_strive.gal_members_summary
--[galvanize_strive_where]
  where campaign_id = '331'
/*  and id in (
  select distinct member_id
  from tmc_strive.gal_groups_members
  where [group_id=Strive_Engagement_Tier]
)*/
  group by 1
  order by 1
  
)

, optouts as (
  
  select coalesce(opt_out_date::date,updated_at::date) as opt_out_date
  --sent_at::date as opt_out_date 
    , count(m.id) as opted_out
from tmc_strive.gal_members_summary m
/*where id in (
  select distinct member_id
  from tmc_strive.gal_groups_members
  where [group_id=Strive_Engagement_Tier]
)*/
where campaign_id = '331'
and opt_in='f'
group by 1

)

, base as (
  
  select 
      opt_out_date
    , opted_out
    , opted_in_date
    , opted_in
    , case when opted_in is null then 0 else opted_in end as opt_ins
    , case when opted_out is null then 0 else opted_out end as opt_outs  
  from list_growth
  full outer join optouts on list_growth.opted_in_date = optouts.opt_out_date
  
)

select *
  , coalesce(opted_in_date,opt_out_date)::date as "date"
  , opt_ins - opt_outs as list_size
from base
order by "date"
