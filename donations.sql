with first_donations as (
   select * from (
   select vanid
   		, datereceived 
      , amount
     	, financialprogramid
      , contributionstatusname
   		, ROW_NUMBER() OVER(PARTITION BY vanid ORDER BY datereceived ASC) as row
   from galvanize.contributions_attributions    
   )
   where row = 1

),

weekly_contributors as (
  select  date_trunc('week', datereceived) as week, t.vanid, initcap(firstname) as firstname, initcap(lastname) as lastname, organizationcontactofficialname, organizationcontactcommonname, amount, datereceived, contributionstatusname
  , case 
  		when financialprogramid = 74 then 'GALA' 
   		when financialprogramid = 73 then 'GALU'
  		when financialprogramid = 77 then 'OFAC'
  else null end as designation
  from first_donations t
  left join tmc_van.gal_contacts_mym m using (vanid)
  )
  
  select * from weekly_contributors
  where week between GETDATE()-7 AND GETDATE()
