with contributions AS (
	select vanid, datereceived, amount, financialprogramid
	from tmc_van.gal_contactscontributions 
	where contributionstatusid = '6' and datecanceled is null 
),

attributions AS (
	select attributedvanid as vanid, datereceived, amountattributed as amount, financialprogramid
    from tmc_van.gal_contactscontributionsattributedcontacts ac
  	left join tmc_van.gal_contactscontributions c using (contactscontributionid)
    where contributionstatusid = '6' and datecanceled is null and datesuppressed is null
),

base AS (
  select * from contributions 
  UNION
  select * from attributions
),

max_date AS (
select vanid, max(datereceived) as most_recent_contrib 
from base
group by 1
),

addresses AS (
  select * 
  from (
      select vanid, city, state 
          , ROW_NUMBER() OVER(PARTITION BY vanid ORDER BY ca.datemodified DESC) as row
      from base b
      left join tmc_van.gal_contactsaddresses_mym ca using (vanid)
        )
  where row = 1
),

activist_codes AS (
select vanid
         , case when activistcodename ilike 'FB Moderator' then 'Y' else null end as moderator_status
         , case when activistcodename ilike '2021 Deep Listening' then 'Y' else null end as dl_2021_status
         , case when activistcodename ilike '2020 Deep Canvasser' then 'Y' else null end as dc_2020_status
from base b 
left join tmc_van.gal_activist_codes_summary_mym ac using (vanid)
where activistcodename in ('FB Moderator','2021 Deep Listening','2020 Deep Canvasser')
  ),
  
donor_tiers as (
  select * from (
  select vanid
  		 , activistcodename as donor_tier
         , row_number() over (partition by vanid order by (case when activistcodename ilike 'LC Current' then '0' when activistcodename ilike 'LC Honorary' then '1' when activistcodename ilike 'Ambassador Current' then '2' when activistcodename ilike 'Ambassador Honorary' then '3' when activistcodename ilike 'LC Past' then '4' when activistcodename ilike 'Ambassador Past' then '5' when activistcodename ilike 'General donor' then '6' else '7' end) asc) as row
from base b
left join tmc_van.gal_activist_codes_summary_mym ac using (vanid)
where activistcodename in ('LC Current', 'LC Honorary','LC Past','Ambassador Current','Ambassador Honorary','Ambassador Past','General Donor')
 )
where row = 1
  ),
  
contribution_sums as (
select * from (
  select vanid, initcap(firstname) as firstname, initcap(lastname) as lastname, organizationcontactofficialname, organizationcontactcommonname, city, state, most_recent_contrib, moderator_status, dl_2021_status, dc_2020_status
         , sum(case when datereceived <= date('2018-12-31') then amount else null end) as amount_2017_and_2018_total
         , sum(case when (datereceived between date('2019-01-01') and date('2019-12-31')) AND (financialprogramid = '74') then amount else null end) as amount_2019_GALA
         , sum(case when (datereceived between date('2019-01-01') and date('2019-12-31')) AND (financialprogramid = '73') then amount else null end) as amount_2019_GALU
         , sum(case when datereceived between date('2019-01-01') and date('2019-12-31') then amount else null end) as amount_2019_TOTAL
         , sum(case when (datereceived between date('2020-01-01') and date('2020-12-31')) AND (financialprogramid = '74') then amount else null end) as amount_2020_GALA
         , sum(case when (datereceived between date('2020-01-01') and date('2020-12-31')) AND (financialprogramid = '73') then amount else null end) as amount_2020_GALU   
         , sum(case when (datereceived between date('2020-01-01') and date('2020-12-31')) AND (financialprogramid = '77') then amount else null end) as amount_2020_OFAC
         , sum(case when datereceived between date('2020-01-01') and date('2020-12-31') then amount else null end) as amount_2020_TOTAL
         , sum(case when (datereceived between date('2021-01-01') and getdate()) AND  (financialprogramid = '74') then amount else null end) as amount_2021_GALA
         , sum(case when (datereceived between date('2021-01-01') and getdate()) AND  (financialprogramid = '73') then amount else null end) as amount_2021_GALU
         , sum(case when (datereceived between date('2021-01-01') and getdate()) AND  (financialprogramid = '77') then amount else null end) as amount_2021_OFAC
         , sum(case when datereceived between date('2021-01-01') and getdate() then amount else null end) as amount_2021_TOTAL
				  , donor_tier
    from base b
 	 left join addresses a using (vanid)
	left join activist_codes ac using (vanid)
	left join max_date md using (vanid)
	left join TMC_VAN.gal_CONTACTS_MYM mym using (vanid)
  left join donor_tiers t using (vanid)
    group by 1,2,3,4,5,6,7,8,9,10,11,24
 )
 where amount_2017_and_2018_total >= 250.00
       OR amount_2019_TOTAL >= 250.00
       OR amount_2020_TOTAL >= 250.00
       OR amount_2021_TOTAL >= 250.00
)

select *
from contribution_sums
where organizationcontactofficialname is null
AND organizationcontactcommonname is null
