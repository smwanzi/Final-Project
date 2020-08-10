select distinct station_id,bikes_available,bikes_disabled,docks_available,docks_disabled,time_reported 
into bikes_nonDups from bikes ;
,count(*) from  bikes group by station_id,bikes_available,bikes_disabled,docks_available,docks_disabled,time_reported having count(*) > 1; 