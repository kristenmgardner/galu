with broadcasts as (
  
  select id as broadcast_id
    , name 
    , sent_at
  from tmc_strive.gal_broadcasts
  where [sent_at=daterange]
  and campaign_id = '331'
  -- The line below excludes broadcasts that we can't identify incoming messages for
  and id in (select distinct broadcast_id from tmc_strive.gal_outgoing_messages where conversation_id is not null)
            
)


, incoming_messages as (
  
  select broadcast_id
    , sum(case when is_opt_out = 't' then 1 else 0 end) as opt_outs 
    , coalesce(count(id),0) as incoming_message_count
    , coalesce(count(distinct(member_id)),0) as unique_respondents
  from tmc_strive.gal_incoming_messages_summary
  [galvanize_strive_where]
  and campaign_id = '331'
  group by 1

)


, flow_actions as (

  select broadcast_id
    , count(fa.id) as flow_actions
    , count(distinct(fa.member_id)) as flow_action_takers
  from tmc_strive.gal_flow_actions fa
  left join (select * from tmc_strive.gal_incoming_messages_summary) i on i.id = fa.incoming_message_id
  where [created_at=daterange]
  and fa.campaign_id = '331'
  group by 1

)


, outgoing_messages as (
  
  select broadcast_id 
    , count(*) as total_texts_sent
    , count(distinct(member_id)) as unique_recipients
    , sum(case when p2p_id is not null then 1 else 0 end) as outgoing_p2p_count
    , sum(case when status in ('undelivered','failed') or error_code in (9,17,18,345,349,350,352,21614,30005) then 1 else 0 end) as opt_outs
  from tmc_strive.gal_outgoing_messages_summary
  [galvanize_strive_where]
  and campaign_id = '331'
  and broadcast_id in (select broadcast_id from broadcasts)
  group by 1

)


, links as (
  
  select broadcast_id
    , sum(case when was_visited = 't' then 1 else 0 end) as link_clicks
    , count(distinct member_id) as unique_clickers
  from tmc_strive.gal_members_links
  where campaign_id = '331'
  and was_visited='t'
  group by 1
  
)   

, engagement as (
 select broadcast_id, count(distinct member_id) as total_engaged
  from (
  select broadcast_id, member_id from tmc_strive.gal_incoming_messages_summary
    UNION
  select broadcast_id, member_id from tmc_strive.gal_members_links
    where was_visited = 't'
    )
  group by 1
  )

select name
  , sent_at::date
  , total_texts_sent as texts_sent
  , unique_recipients
  , coalesce(im.incoming_message_count,0) as responses
  , coalesce(im.unique_respondents,0) as unique_respondents
  , coalesce(flow_actions,0) as flow_actions
  , coalesce(flow_action_takers,0) as flow_action_takers
  , coalesce(link_clicks,0) as link_clicks
  , im.opt_outs + om.opt_outs as opt_outs
  , total_engaged
from broadcasts
left join outgoing_messages om using(broadcast_id)
left join incoming_messages im using(broadcast_id)
left join flow_actions using(broadcast_id)
left join links using(broadcast_id)
left join engagement using(broadcast_id)
where total_texts_sent is not null
order by sent_at desc
