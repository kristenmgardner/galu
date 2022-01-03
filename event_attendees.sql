SELECT date(date_trunc('week', date(e.dateoffsetbegin))) as Week
     , count(case when eventrolename = '1:1 Attendee' then vanid else null end) as one_on_one
     , count(case when eventrolename = 'Attendee' then vanid else null end) as event_attendee 
from tmc_van.gal_eventsignups es
left join tmc_van.gal_eventsignupsstatuses ess on (es.eventsignupid = ess.eventsignupid)
left join tmc_van.gal_events e on (es.eventid = e.eventid)
where eventstatusname = 'Completed'
and [e.dateoffsetbegin=DateRange]
and datetimeoffsetbegin >= DATE('2021-01-01')
and es.datesuppressed is null
and e.datesuppressed is null
and committeename = 'Galvanize (TMC)'
GROUP BY 1 ORDER BY 1
