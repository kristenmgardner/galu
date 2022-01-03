with base as (
select * from (
  select vanid
         , sum(case when datereceived between date('2020-01-01') and date('2020-12-31') then amount else null end) as amount_2020_TOTAL
    from galvanize.contributions_attributions
    group by 1
   )  
where amount_2020_TOTAL >= 250.00 
  ),
  
 min_donations as (
   select * from (
   select b.vanid
   		 , datereceived
       , amount
       , financialprogramid
       , contributionstatusname
   	 	 , ROW_NUMBER() OVER(PARTITION BY vanid ORDER BY datereceived ASC) as row
   from base b
   left join galvanize.contributions_attributions ca using (vanid)  
   where datereceived between date('2021-01-01') and getdate()
   )
   where row = 1
   ),
   
 weekly_renewals as (
  select date_trunc('week', datereceived) as week, d.vanid, initcap(firstname) as firstname, initcap(lastname) as lastname, organizationcontactofficialname, organizationcontactcommonname, amount, datereceived, contributionstatusname
  , case 
  		when financialprogramid = 74 then 'GALA' 
   		when financialprogramid = 73 then 'GALU'
  		when financialprogramid = 77 then 'OFAC'
  else null end as designation
  from min_donations d
  left join tmc_van.gal_contacts_mym m using (vanid)
  )
  
  select * from weekly_renewals
  where week between GETDATE()-7 AND GETDATE()
