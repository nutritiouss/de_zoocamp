-- show largre net managers by num connections
SELECT net_manager, count(num_connections) as num_connections
FROM `netherlands-energy.energy_data.bq_energy_dataset`
group by net_manager
order by 2 Desc


-- SELECT year ,avg(delivery_perc) as delivery_perc
-- FROM `netherlands-energy.energy_data.bq_energy_dataset`
-- group by year
-- order by 1
-- SELECT year,net_manager,avg(delivery_perc) as delivery_percr
-- FROM `netherlands-energy.energy_data.bq_energy_dataset`
-- where $__timeFilter(year) and net_manager in ('Enexis B.V.', 'Liander N.V.')
-- group by year,net_manager
-- order by 1

select year,
     max(case when  net_manager = 'Enexis B.V.' then delivery_perc else null end)  as enexis ,
     max(case when  net_manager = 'Liander N.V.' then delivery_perc else null end) as liander
 from (
      SELECT year,net_manager,avg(num_connections) as delivery_perc
      FROM `netherlands-energy.energy_data.bq_energy_dataset`
      where net_manager in ('Enexis B.V.', 'Liander N.V.')
      group by year,net_manager
)
where $__timeFilter(year)
group by year
order by 1
