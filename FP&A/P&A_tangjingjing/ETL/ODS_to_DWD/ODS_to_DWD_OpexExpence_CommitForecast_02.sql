drop table temp_capexforecast
show procedure ss_rs_ts_aut_ap_digital_schema.sp_Finance_FPA_Opex_ODS_to_DWD_OpexExpenceCommitForecastv2()
call ss_rs_ts_aut_ap_digital_schema.sp_Finance_FPA_Opex_ODS_to_DWD_OpexExpenceCommitForecastv2()
CREATE OR REPLACE PROCEDURE ss_rs_ts_aut_ap_digital_schema.sp_Finance_FPA_Opex_ODS_to_DWD_OpexExpenceCommitForecastv2()
LANGUAGE plpgsql
AS $$
BEGIN

create temporary table Temp_ForecastLogic as
select
b.fisical_YearMonth,
e.Count_FW,
c."Team",
d.scenario,
d.Des,
d."index",
start_fiscalYear,
End_fiscalYear,
"Expense level 1",
"Expense level 2",
"Acc",	
"Accounts/Cost Elements",	
"Commit Logic base actual",	
"Commit Index base actual",	
"Commit Logic base forecast",	
"Commit Index base forecast"
from 
(
select 
1 as id,
start_fiscalYear,
End_fiscalYear,
"Expense level 1",
"Expense level 2",	
"Acc",	
"Accounts/Cost Elements",	
"Commit Logic base actual",	
"Commit Index base actual",	
"Commit Logic base forecast",	
"Commit Index base forecast"
from s3dataschema.ods_Finance_FPA_Opex_ForecastLogic
where Acc<>'Acc'
) a 

left join
(
   select
   a1.fisical_Year||b1.fisical_Month  as fisical_YearMonth
   from
	(
	select
	distinct
    1 as id,
	left(fisical_YearMonth,4) as fisical_Year
	from ss_rs_ts_aut_ap_digital_schema.DWS_Finance_FPA_Opex_ActualInternal_FinalActual
	WHERE coalesce(Amount_CNY,0)<>0 AND LEFT(fisical_YearMonth,4)>=2025
	) a1

	left join
	(
	select
	distinct
    1 as id,
	right(fisical_YearMonth,2) as fisical_Month
	from ss_rs_ts_aut_ap_digital_schema.DWS_Finance_FPA_Opex_ActualInternal_FinalActual
	WHERE coalesce(Amount_CNY,0)<>0 
	) b1
  on a1.id=b1.id
) b
on left(b.fisical_YearMonth,4) between a.start_fiscalYear and a.End_fiscalYear

left join
(
select
distinct
1 as id,
Team
from ss_rs_ts_aut_ap_digital_schema.DWS_Finance_FPA_Opex_ActualInternal_FinalActual
WHERE team is not null

) c 
on a.id=c.id

left join 

(
select
distinct
1 as id,
scenario,
Des,
"index"
from s3dataschema.ods_Finance_FPA_Opex_ScenarioMap
where scenario<>'Scenario'
) d 
on a.id=d.id

LEFT JOIN 

(
select 
fiscal_year_month_id,
count(distinct fiscal_year_week_id) as Count_FW
from ss_rs_ts_aut_ap_digital_db."master_data"."dimension_date"  
where fiscal_year_month_id>=2025
GROUP BY fiscal_year_month_id
) e
ON b.fisical_YearMonth=e.fiscal_year_month_id
where left(b.fisical_YearMonth,4)||right(cast(d."index" as int)+100,2)>=202510  -- 2025 AugRnO 
and left(b.fisical_YearMonth,4)||right(cast(d."index" as int)+100,2)<=
   (select
	max(fisical_YearMonth) as fisical_YearMonth
	from ss_rs_ts_aut_ap_digital_schema.DWS_Finance_FPA_Opex_ActualInternal_FinalActual
	WHERE coalesce(Amount_CNY,0)<>0 AND LEFT(fisical_YearMonth,4)>=2025
  )
;



create temporary table all_dimension as 
select
a.YearMonth,
b.Team,
c.Account
from
(
select
distinct
1 as id,
fisical_YearMonth as YearMonth
from ss_rs_ts_aut_ap_digital_schema.DWS_Finance_FPA_Opex_ActualInternal_FinalActual
where LEFT(fisical_YearMonth,4)>=2024
) a

left join 

(
select
distinct 
1 as id,
Team
from ss_rs_ts_aut_ap_digital_schema.DWS_Finance_FPA_Opex_ActualInternal_FinalActual
where LEFT(fisical_YearMonth,4)>=2024
) b
on a.id=b.id

left join 

(
select
distinct 
1 as id,
Account
from ss_rs_ts_aut_ap_digital_schema.DWS_Finance_FPA_Opex_ActualInternal_FinalActual
where LEFT(fisical_YearMonth,4)>=2024
) c
on a.id=c.id;


create temporary table Temp_Actual as
select
a.YearMonth,
a.Team,
a.Account,
b.Amount_CNY,
c.Count_FW
from
(
select
YearMonth,
Team,
Account
from all_dimension 
) a
left join
(
select
fisical_YearMonth as YearMonth,
to_char(add_months(date(fisical_YearMonth||'01'),1),'YYYYMM') as lastYearmonth,
Team,
Account,
sum(Amount_CNY) as Amount_CNY
from ss_rs_ts_aut_ap_digital_schema.DWS_Finance_FPA_Opex_ActualInternal_FinalActual
where LEFT(fisical_YearMonth,4)>=2024
group by 
fisical_YearMonth,
team,
Account
) b
on a.YearMonth=b.YearMonth and a.Team=b.Team and a.Account=b.Account

LEFT JOIN 

(
select 
fiscal_year_month_id,
count(distinct fiscal_year_week_id) as Count_FW
from ss_rs_ts_aut_ap_digital_db."master_data"."dimension_date"  
where fiscal_year_month_id>=2025
GROUP BY fiscal_year_month_id
) c
ON a.YearMonth=c.fiscal_year_month_id;

--HC Plan
create temporary table Temp_ForecastHC as 
select
Scenario,
Fyear||right(cast(replace(lower(Months),'p','') as int)+100,2) as F_YearMonth,
Team,
quatity
from
(
select
split_part(Filename,'_',1) as Scenario,
left(Filename,4) as Fyear,
Team,
P1,
P2,
P3,
P4,
P5,
P6,
P7,
P8,
P9,
P10,
P11,
P12
from s3dataschema.ods_Finance_FPA_Opex_ForecastHeadCount
where team<>'Team'
) a
unpivot(quatity for Months in ("P1","P2","P3","P4","P5","P6","P7","P8","P9","P10","P11","P12"));


create temporary table Temp_Forecast_YTDHC AS 
select
a2.Scenario, 
left(a2.Scenario,4)||"index" as ScenarioYM,
a2.F_YM ,
a1.Team,
a.quatity,
a.YTD_quatity_f,
c.YTD_quatity_a,
current_timestamp as timestamps
from 
(
select 
distinct
1 as id,
Team_new AS Team
FROM ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_Actual_HC
where Team_new<>''
) a1

left join
(
select
distinct
1 as id,
Scenario,
F_YearMonth as F_YM
from Temp_ForecastHC
where quatity<>''
) a2
on a1.id=a2.id

left join
(
select
Scenario,
F_YearMonth as F_YM,
Team,
cast(quatity as float) as quatity,
sum(cast(quatity as float))over(partition by Scenario,Team order by F_YearMonth asc rows between unbounded preceding and current row) as YTD_quatity_f
from Temp_ForecastHC
where quatity<>''
) a 
on a1.Team=a.Team and a2.Scenario=a.Scenario and a2.F_YM=a.F_YM

left join

(
select
distinct
scenario,
"index"
from s3dataschema.ods_Finance_FPA_Opex_ScenarioMap
where scenario<>'Scenario'
) b 
on split_part(a2.Scenario,' ',2)=b.scenario


left join

(
select 
F_YM,
Team_new AS Team,
sum(cast(quatity as float)) as YTD_quatity_a
FROM ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_Actual_HC
group by 
F_YM,
Team_new
) c
on left(a2.Scenario,4)||b."index"=c.F_YM AND a1.team=c.team;

delete ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_ActualForecast_HC;
insert into ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_ActualForecast_HC
(
Scenario,
F_YM,
"Cost Center",
Name,
Region,
"Company Code",
"Profit Center",
"Person Responsible",
Category,
Department,
Team,
Mark,
quatity,
timestamps
)
SELECT
left(F_YM,4)||' 12+0' as Scenario,
F_YM,
"Cost Center",
Name,
Region,
"Company Code",
"Profit Center",
"Person Responsible",
Category,
Department,
Team,
Mark,
cast(quatity as float) as quatity,
current_timestamp as timestamps
FROM ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_Actual_HC

union all

select
Scenario,
F_YM,
Team as "Cost Center",
'' as Name,
'' as Region,
'' as "Company Code",
'' as "Profit Center",
'' as "Person Responsible",
'' as Category,
'' as Department,
Team,
'' as Mark,
coalesce(YTD_quatity_f,0)+coalesce(YTD_quatity_a,0) as quatity,
current_timestamp as timestamps
from Temp_Forecast_YTDHC;

DROP TABLE Temp_Forecast_YTDHC;
--Labor adj
create temporary table Temp_Forecast_LaborAdj as 
select
a.Scenario,
left(a.Scenario,4)||right(cast(b."index" as int)+100,2) as scenario_YM,
F_YearMonth,
Team,
Acc,
Amount
from
(
select
Scenario,
Fyear||right(cast(replace(lower(Months),'p','') as int)+100,2) as F_YearMonth,
Team,
Acc,
case when Amount='' or Amount is null then '0' else Amount end as Amount
from
(
select
split_part(Filename,'_',1) as Scenario,
left(Filename,4) as Fyear,
Team,
Acc,
P1,
P2,
P3,
P4,
P5,
P6,
P7,
P8,
P9,
P10,
P11,
P12
from s3dataschema.ods_Finance_FPA_Opex_ForecastLaborAdj
where team<>'Team'
) a1
unpivot(amount for Months in ("P1","P2","P3","P4","P5","P6","P7","P8","P9","P10","P11","P12"))
) a
left join 

(
select
distinct
scenario,
Des,
"index"
from s3dataschema.ods_Finance_FPA_Opex_ScenarioMap
where "index"<>'index'
) b
on split_part(a.Scenario,' ',2)=b.scenario;
--Labor total amount

create temporary table Temp_Labor_total as 
select
a.Scenario,
a.scenario_YM,
a.F_YearMonth,
a.Team,
a.Amount,
b.quatity,
a.Amount*b.quatity as Labor_Amount,
sum(a.Amount*b.quatity)over(partition by a.Scenario,a.Team order by a.F_YearMonth asc rows between unbounded preceding and current row) as YTD_Labor_Amount
from

(
select
Scenario,
scenario_YM,
F_YearMonth,
Team,
--Acc,
max(Amount) as Amount
from Temp_Forecast_LaborAdj
group by Scenario,
scenario_YM,
F_YearMonth,
Team
) a 

left join 

(
select
Scenario,
F_YearMonth,
Team,
max(quatity) as quatity
from Temp_ForecastHC
where quatity<>'' and quatity is not null
group by Scenario,
F_YearMonth,
Team
) b
on a.Scenario=b.Scenario   and a.F_YearMonth=b.F_YearMonth  and a.Team=b.Team;

create temporary table Temp_Actual_YTD AS 
select
b2.YearMonth,
b2.Count_FW,
b1.Team,
b1.Account,
b3.Amount_CNY,
sum(b3.Amount_CNY) over(partition by b1.Team,b1.Account,left(b2.YearMonth,4) order by b2.YearMonth rows between unbounded preceding and current row) as YTD_Actual_Amount_CNY
from
(
select
distinct
1 as id,
Team,
Account
from Temp_Actual
) b1

left join

(
select
distinct
1 as id,
YearMonth,
Count_FW
from Temp_Actual 
) b2
on b1.id=b2.id

left join

(
select
YearMonth,
Team,
Account,
Amount_CNY
from Temp_Actual
) b3
on b1.Team=b3.Team and b1.Account=b3.Account and b2.YearMonth=b3.YearMonth;


--Labor base on Actual
delete ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_Forecast_LaborAllocation_BaseonActual;
insert into ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_Forecast_LaborAllocation_BaseonActual
(
Scenario,
F_YearMonth,
Team,
Acc,
YTD_Actual_Amount_CNY,
Total_YTD_Actual_Amount_CNY,
YTD_Labor_Amount,
ratio,
Labor_Amount,
timestamps
)
select
a.Scenario,
c.F_YearMonth,
a.Team,
a.Acc,
coalesce(b.YTD_Actual_Amount_CNY,0) as YTD_Actual_Amount_CNY,
sum(b.YTD_Actual_Amount_CNY)over(partition by a.Scenario,c.F_YearMonth,a.Team) as Total_YTD_Actual_Amount_CNY,
c.YTD_Labor_Amount,
coalesce(b.YTD_Actual_Amount_CNY,0)/coalesce(cast(sum(b.YTD_Actual_Amount_CNY)over(partition by a.Scenario,c.F_YearMonth,a.Team) as float),1) as ratio,
coalesce(b.YTD_Actual_Amount_CNY,0)/coalesce(cast(sum(b.YTD_Actual_Amount_CNY)over(partition by a.Scenario,c.F_YearMonth,a.Team) as float),1)
*c.YTD_Labor_Amount as Labor_Amount,
current_timestamp as timestamps
from
(
select
distinct
Scenario,
scenario_YM,
Team,
Acc,
'Y' as Labor_flag
from Temp_Forecast_LaborAdj

) a

left join
(
select
YearMonth,
Team,
Account,
--Amount_CNY,
YTD_Actual_Amount_CNY
from Temp_Actual_YTD
) b 
on a.scenario_YM=b.YearMonth and a.Team=b.Team and a.Acc=b.Account

left join 

(
select
Scenario,
F_YearMonth,
Team,
YTD_Labor_Amount
from Temp_Labor_total
where coalesce(YTD_Labor_Amount,0)<>0
) c
on a.Scenario=c.Scenario  and a.Team=c.Team
where F_YearMonth is not null;


--Labor base on Outlook
delete ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_Forecast_LaborAllocation_BaseonForecast;
insert into ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_Forecast_LaborAllocation_BaseonForecast
(
Scenario,
scenario_YM,
F_YearMonth,
Team,
Acc,
YTD_Budget_Amount_CNY,
YTD_Labor_Amount,
Total_YTD_Budget_Amount_CNY,
ratio,
Labor_Amount,
timestamps
)
select
a.Scenario,
a.scenario_YM,
c.F_YearMonth,
a.Team,
a.Acc,
b.YTD_Budget_Amount_CNY,
c.YTD_Labor_Amount,
sum(b.YTD_Budget_Amount_CNY)over(partition by a.Scenario,c.F_YearMonth,a.Team) as Total_YTD_Budget_Amount_CNY,
b.YTD_Budget_Amount_CNY/cast(coalesce(sum(b.YTD_Budget_Amount_CNY)over(partition by a.Scenario,c.F_YearMonth,a.Team),1) as float) as ratio,
b.YTD_Budget_Amount_CNY/cast(coalesce(sum(b.YTD_Budget_Amount_CNY)over(partition by a.Scenario,c.F_YearMonth,a.Team),1) as float)
*c.YTD_Labor_Amount as Labor_Amount,
current_timestamp as timestamps
from 
(
select
distinct
Scenario,
scenario_YM,
Team,
Acc,
'Y' as Labor_flag
from Temp_Forecast_LaborAdj
) a

left join 
(
select
left(Scenario,4) as Scenario_Year,
Team,
Acc,
sum(Budget_Final) as YTD_Budget_Amount_CNY
from ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_Forecast_FactVersion1
group by
left(Scenario,4),
Team,
Acc
HAVING COALESCE(sum(Budget_Final),0)<>0
) b 
on left(a.Scenario,4)=b.Scenario_Year and a.Team=b.Team and a.Acc=b.Acc

left join 

(
select
Scenario,
F_YearMonth,
Team,
YTD_Labor_Amount
from Temp_Labor_total
where coalesce(YTD_Labor_Amount,0)<>0
) c
on a.Scenario=c.Scenario  and a.Team=c.Team
where F_YearMonth is not null;

--Manual Adj 
create temporary table Temp_Forecast_ManualAdj as 
select
Scenario,
Fyear||right(cast(replace(lower(Months),'p','') as int)+100,2) as F_YearMonth,
Team,
Acc,
amount
from
(
select
split_part(Filename,'_',1) as Scenario,
left(Filename,4) as Fyear,
Team,
Acc,
P1,
P2,
P3,
P4,
P5,
P6,
P7,
P8,
P9,
P10,
P11,
P12
from s3dataschema.ods_Finance_FPA_Opex_ForecastManualAdj
where Team<>'Team'
) a
unpivot(amount for Months in ("P1","P2","P3","P4","P5","P6","P7","P8","P9","P10","P11","P12"))
where coalesce(cast(amount as float),0)<>0;


--Capex
create temporary table Temp_CapexForecast as 
select 
Scenario,
Year||right(fisical_Month,2) as fisical_YearMonth,
--"CER / external order number",
--"Description",
--"Cost Center",
--"IO/PO Responsible",
"Department",
"Acc",
budget
from 
(
select 
split_part(Filename,'_',1) as Scenario,
left(Filename,4) as Year,
Filename,
--"CER / external order number",
--"Description",
--"Cost Center",
--"IO/PO Responsible",
"Department",
"Acc",
cast(P1 as float) as P01,
cast(P2 as float) as P02,
cast(P3 as float) as P03,
cast(P4 as float) as P04,
cast(P5 as float) as P05,
cast(P6 as float) as P06,
cast(P7 as float) as P07,
cast(P8 as float) as P08,
cast(P9 as float) as P09,
cast(P10 as float) as P10,
cast(P11 as float) as P11,
cast(P12 as float) as P12
from s3dataschema.ods_Finance_FPA_Opex_CapexForecast
where sheetname<>'sheetname' and Filename not like '%Outlook%'
) as a 
unpivot(budget for fisical_Month in ("P01","P02",P03,P04,P05,P06,P07,P08,P09,P10,P11,P12))
where coalesce(cast(budget as float),0)<>0;

--LeaseForecast
create temporary table Temp_LeaseForecast as 
select 
Scenario,
Year||right(fisical_Month,2) as fisical_YearMonth,
"index",
"Acc",
"Description",
"Business Unit",
"Cost Center",
"Department",
"Start FYFM",
"End FYFM",
"period",
budget
from 
(
select
split_part(Filename,'_',1) as Scenario,
left(Filename,4) as Year,
"index",
"Acc",
"Description",
"Business Unit",
"Cost Center",
"Department",
"Start FYFM",
"End FYFM",
"period",
"Total Leasing Amount",
P1 as P01,
P2 as P02,
P3 as P03,
P4 as P04,
P5 as P05,
P6 as P06,
P7 as P07,
P8 as P08,
P9 as P09,
P10 as P10,
P11 as P11,
P12 as P12
from s3dataschema.ods_Finance_FPA_Opex_LeaseForecast 
where index<>'Index'  and Filename not like '%Outlook%'
) as a 
unpivot(budget for fisical_Month in ("P01","P02",P03,P04,P05,P06,P07,P08,P09,P10,P11,P12))
where coalesce(cast(budget as float),0)<>0;

--EngineeringAllocations
create temporary table Temp_EngineeringAllocations as 
select 
Scenario,
Year||right(fisical_Month,2) as fisical_YearMonth,
"Expense level 1",
"Expense level 2",	
Department,
Acc	,	
"Accounts/Cost Elements",
budget
from 
(
select
split_part(Filename,'_',1) as Scenario,
left(Filename,4) as Year,
"Expense level 1",
"Expense level 2",	
Department,
Acc,	
"Accounts/Cost Elements",
P1 as P01,
P2 as P02,
P3 as P03,
P4 as P04,
P5 as P05,
P6 as P06,
P7 as P07,
P8 as P08,
P9 as P09,
P10 as P10,
P11 as P11,
P12 as P12
from s3dataschema.ods_Finance_FPA_Opex_EngineeringAllocations 
where acc<>'Acc' AND Filename not LIKE '%Outlook%'
) as a 
unpivot(budget for fisical_Month in ("P01","P02",P03,P04,P05,P06,P07,P08,P09,P10,P11,P12))
where budget<>0 and  budget is not null and budget<>'' ;

--ActualYTD
create temporary table Temp_ActualYTD AS 
select
Years,
fisical_YM,
sum(Amount)over(partition by Years order by fisical_YM rows between unbounded preceding and current row) as YTD_Actual_Sales
from
(
select
left(fisical_YM,4) as Years,
fisical_YM,
sum(Amount) as amount
from s3dataschema.ods_Finance_FPA_Opex_Sales
where filename like '%Actual%'
group by fisical_YM
) a;



create temporary table Temp_CommitRno as 
select
a.scenario,
a.scenario_YM,
a.fisical_YearMonth,
a.Count_FW,
a."Team",
Des,
"index",
start_fiscalYear,
End_fiscalYear,
"Expense level 1",
"Expense level 2",	
a."Acc",	
"Accounts/Cost Elements",	
"Commit Logic base actual",	
"Commit Index base actual",	
"Commit Logic base forecast",	
"Commit Index base forecast",
b.last_actual_amount,
c.Labor_Amount as Labor_Amount_baseActual,
c2.Labor_Amount as Labor_Amount_baseOutlook,
d.YTD_Amount_CNY,
d.YTD_Count_FW,
d.YTD_Amount_CNY/d.YTD_Count_FW*a.Count_FW as AvgFW_budget_baseonActual,
d.Avg_Actual_YTD_Amount_CNY,
e.Manual_Forecast_Adj,
f.Capex_Forecast_budget,
g.Lease_Forecast_budget,
h1.YTD_Actual_Sales,
h2.last_Actual_salesamount,
h3.forecast_sales_amount,
h3.forecast_sales_amount_YTD,
j.outlook_Budget_Final,
j.Total_Budget_Final,
(coalesce(j.Total_Budget_Final,0)-coalesce(d.YTD_Amount_CNY,0))/(12-cast(right(scenario_YM,2) as int)) as AvgOutlook_vs_Actual_baseonActual,
k.EngineeringAllocations_forecast_manualAdj,
r.Des1,
r.Des2,
current_timestamp as timestamps
from
(
select 
distinct
left(fisical_YearMonth,4)||' '||scenario as scenario,
left(fisical_YearMonth,4)||right(cast("index" as int)+100,2) as scenario_YM,
fisical_YearMonth,
Count_FW,
"Team",
Des,
"index",
start_fiscalYear,
End_fiscalYear,
"Expense level 1",
"Expense level 2",	
"Acc",	
"Accounts/Cost Elements",	
"Commit Logic base actual",	
"Commit Index base actual",	
"Commit Logic base forecast",	
"Commit Index base forecast"
 from Temp_ForecastLogic
where fisical_YearMonth>left(fisical_YearMonth,4)||right(cast("index" as int)+100,2) 
) a 


left join 

--Actual
(
select
Yearmonth,
Team,
Account,
sum(coalesce(Amount_CNY,0)) as last_actual_amount
from Temp_Actual
group by Yearmonth,
Team,
Account
) b 
on a.scenario_YM=b.Yearmonth and a."Team"=b."Team" and a.Acc=b.Account


left join 
--HC Plan
(
select
Scenario,
F_YearMonth,
Team,
Acc,
sum(Labor_Amount) as Labor_Amount
from ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_Forecast_LaborAllocation_BaseonActual
group by 
Scenario,
F_YearMonth,
Team,
Acc
) c 
on a.Scenario=c.Scenario and a.fisical_YearMonth=c.F_YearMonth and a.Team=c.Team and a.Acc=c.Acc

left join
(
select
Scenario,
F_YearMonth,
Team,
Acc,
sum(Labor_Amount) as Labor_Amount
from ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_Forecast_LaborAllocation_BaseonForecast
--where F_YearMonth=202511 and Team='SZ RD' and acc='6023000'
group by Scenario,
F_YearMonth,
Team,
Acc
) c2
on a.Scenario=c2.Scenario and a.fisical_YearMonth=c2.F_YearMonth and a.Team=c2.Team and a.Acc=c2.Acc

left join
--Actual YTD
(
select
YearMonth,
Count_FW,
Team,
Account,
sum(Amount_CNY) over(partition by Team,Account,left(YearMonth,4) order by YearMonth rows between unbounded preceding and current row) as YTD_Amount_CNY,
sum(Count_FW) over(partition by Team,Account,left(YearMonth,4) order by YearMonth rows between unbounded preceding and current row) as YTD_Count_FW,
cast(right(YearMonth,2) as int) as counts,
sum(Amount_CNY) over(partition by Team,Account,left(YearMonth,4) order by YearMonth rows between unbounded preceding and current row)
/cast(right(YearMonth,2) as int) as Avg_Actual_YTD_Amount_CNY
from Temp_Actual
) d 
on a.scenario_YM=d.YearMonth and a."Team"=d."Team" and a.Acc=d.Account


left join 
--Manual Adj
(
select
replace(Scenario,'outlook','Outlook') as Scenario,
F_YearMonth,
Team,
Acc,
sum(coalesce(cast(amount as float),0)) as Manual_Forecast_Adj
from Temp_Forecast_ManualAdj
where coalesce(cast(amount as float),0) <>0
group by replace(Scenario,'outlook','Outlook'),
F_YearMonth,
Team,
Acc
) e
on a.Scenario=e.Scenario and a.fisical_YearMonth=e.F_YearMonth and a.Team=e.Team and a.Acc=e.Acc

left join
--CapexForecast
(
select 
Scenario,
fisical_YearMonth,
"Department" as Team,
Acc,
sum(budget) as Capex_Forecast_budget
from Temp_CapexForecast
group by 
Scenario,
fisical_YearMonth,
"Department",
Acc
) f
on a.Scenario=f.Scenario and a.fisical_YearMonth=f.fisical_YearMonth and a.Team=f.Team AND a.Acc=f.Acc


--leasing Forecast
left join
(
select 
Scenario,
fisical_YearMonth,
"Acc",
"Department" as Team,
sum(budget) as Lease_Forecast_budget
from Temp_LeaseForecast
group by 
Scenario,
fisical_YearMonth,
"Acc",
"Department"
) g
on a.Scenario=g.Scenario and a.Acc=g.Acc and a.fisical_YearMonth=g.fisical_YearMonth and a.Team=g.Team 


--sales actual
left join 
(
select
Years,
fisical_YM,
sum(YTD_Actual_Sales) as YTD_Actual_Sales
from Temp_ActualYTD
group by 
Years,
fisical_YM
) h1
on a.scenario_YM=h1.fisical_YM



left join 
(
select
fisical_YM,
--to_char(add_months(date(fisical_YM||'01'),-1),'YYYYMM') as fisical_YM_Key,
sum(Amount) as last_Actual_salesamount
from s3dataschema.ods_Finance_FPA_Opex_Sales
where filename like '%Actual%'  and fisical_ym<>'Fiscal' and fisical_ym<>''
group by fisical_YM
) h2
on a.scenario_YM=h2.fisical_YM

--SaleForecast
left join
(
select
Scenario,
fisical_YM,
forecast_sales_amount,
sum(forecast_sales_amount)over(partition by Scenario order by fisical_YM rows between unbounded preceding and current row) as forecast_sales_amount_YTD
from
(
select
split_part(filename,'_',1) as Scenario,
fisical_YM,
sum(Amount) as forecast_sales_amount
from s3dataschema.ods_Finance_FPA_Opex_Sales
where filename not like '%Actual%' and fisical_ym<>'Fiscal'
group by split_part(filename,'_',1) ,fisical_YM
) a1
) h3
on a.Scenario=h3.Scenario and  a.fisical_YearMonth=h3.fisical_YM

--base on budget
left join
(
	select
	fisical_YearMonth,
	Team,
	"Acc",
	outlook_Budget_Final,
	sum(outlook_Budget_Final) over(partition by Team,"Acc",left(fisical_YearMonth,4)) as Total_Budget_Final
	from
	(
	select
	--Scenario,
	--to_char(add_months(date(fisical_YearMonth||'01'),-12),'YYYYMM') as fisical_YearMonth_key,
	fisical_YearMonth,
	Team,
	"Acc",	
	sum(Budget_Final) as outlook_Budget_Final
	from ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_Forecast_FactVersion1
	where coalesce(Budget_Final,0)<>0 
	group by 
	--Scenario,
	fisical_YearMonth,
	Team,
	"Acc"
	) x
) j
on a.fisical_YearMonth=j.fisical_YearMonth and a.Team=j.Team and a.Acc=j.Acc

--EngineeringAllocations
left join 
(
select 
Scenario,
fisical_YearMonth,
Department as Team,
Acc	,	
sum(budget) as EngineeringAllocations_forecast_manualAdj
from Temp_EngineeringAllocations
group by 
Scenario,
fisical_YearMonth,
Team,
Acc
) k
on a.Scenario=k.Scenario and a.fisical_YearMonth=k.fisical_YearMonth and a.Team=k.Team and a.Acc=k.Acc


--标识
left join 
(

select
Year,
b."index" as Des1,
c."index" as Des2
from
(
select
right(Fyear,4) as Year,
Des as Des1,
Des2
 from s3dataschema.ods_Finance_FPA_Opex_parameter
where fyear<>'Fyear'
) a

left join 
(
select
distinct
1 as id,
scenario,
Des,
"index"
from s3dataschema.ods_Finance_FPA_Opex_ScenarioMap
where scenario<>'Scenario'
) b
on a.Des1=b.scenario

left join 
(
select
distinct
1 as id,
scenario,
Des,
"index"
from s3dataschema.ods_Finance_FPA_Opex_ScenarioMap
where scenario<>'Scenario'
) c
on a.Des2=c.scenario
) r
on left(a.scenario_YM,4)=r.Year;

drop table Temp_ForecastLogic;
drop table Temp_Actual;
drop table Temp_ForecastHC;
drop table Temp_Forecast_ManualAdj;
drop table Temp_CapexForecast;
drop table Temp_LeaseForecast;
drop table Temp_ActualYTD;
drop table Temp_EngineeringAllocations;
drop table Temp_Forecast_LaborAdj;
drop table Temp_Actual_YTD;
drop table Temp_Labor_total;
drop table all_dimension;




delete ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_Forecast_CommitRnoForecast;
insert into ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_Forecast_CommitRnoForecast
(
scenario,
scenario_YM,
fisical_YearMonth,
"Team",
Des,
"index",
start_fiscalYear,
End_fiscalYear,
"Expense level 1",
"Expense level 2",	
"Acc",	
"Accounts/Cost Elements",	
"Commit Logic base actual",	
"Commit Index base actual",	
"Commit Logic base forecast",	
"Commit Index base forecast",
last_actual_amount,
Labor_Amount_baseActual,
Labor_Amount_baseOutlook,
YTD_Amount_CNY,--new
YTD_Count_FW,--new
Avg_Actual_YTD_Amount_CNY,
Manual_Forecast_Adj,
Capex_Forecast_budget,
Lease_Forecast_budget,
YTD_Actual_Sales,
last_Actual_salesamount,
forecast_sales_amount,
forecast_sales_amount_YTD,
Total_Budget_Final,--new
outlook_Budget_Final,
EngineeringAllocations_forecast_manualAdj,
AvgOutlook_vs_Actual_baseonActual, --new
AvgFW_budget_baseonActual, --new
des1,
des2,
Budget_BaseonActual,
Budget_Baseonoutlook,
CommitRno_ForcastAmount,
timestamps
)
select
scenario,
scenario_YM,
fisical_YearMonth,
"Team",
Des,
"index",
start_fiscalYear,
End_fiscalYear,
"Expense level 1",
"Expense level 2",	
"Acc",	
"Accounts/Cost Elements",	
"Commit Logic base actual",	
"Commit Index base actual",	
"Commit Logic base forecast",	
"Commit Index base forecast",
last_actual_amount,
Labor_Amount_baseActual,
Labor_Amount_baseOutlook,
YTD_Amount_CNY,--new
YTD_Count_FW,--new
Avg_Actual_YTD_Amount_CNY,
Manual_Forecast_Adj,
Capex_Forecast_budget,
Lease_Forecast_budget,
YTD_Actual_Sales,
last_Actual_salesamount,
forecast_sales_amount,
forecast_sales_amount_YTD,
Total_Budget_Final,--new
outlook_Budget_Final,
EngineeringAllocations_forecast_manualAdj,
AvgOutlook_vs_Actual_baseonActual,--new
AvgFW_budget_baseonActual,--new
des1,
des2,
Budget_BaseonActual,
Budget_Baseonoutlook,
case when cast("Index" as int)<=cast(des1 as int) then Budget_Baseonoutlook else Budget_BaseonActual end as CommitRno_ForcastAmount,
current_timestamp as timestamps
from
(
select
scenario,
scenario_YM,
fisical_YearMonth,
"Team",
Des,
"index",
start_fiscalYear,
End_fiscalYear,
"Expense level 1",
"Expense level 2",	
"Acc",	
"Accounts/Cost Elements",	
"Commit Logic base actual",	
"Commit Index base actual",	
"Commit Logic base forecast",	
"Commit Index base forecast",
last_actual_amount,
Labor_Amount_baseActual,
Labor_Amount_baseOutlook,
YTD_Amount_CNY,--new
YTD_Count_FW,--new
Avg_Actual_YTD_Amount_CNY,
Manual_Forecast_Adj,
Capex_Forecast_budget,
Lease_Forecast_budget,
YTD_Actual_Sales,
last_Actual_salesamount,
forecast_sales_amount,
forecast_sales_amount_YTD,
Total_Budget_Final,--new
outlook_Budget_Final,
EngineeringAllocations_forecast_manualAdj,
AvgOutlook_vs_Actual_baseonActual,--new
AvgFW_budget_baseonActual,--new
des1,
des2,
case when "Commit Index base actual"=1 then last_actual_amount 
     when "Commit Index base actual"=2 then coalesce(last_actual_amount,0)+coalesce(Labor_Amount_baseActual,0) --还有个HC 逻辑待确定
	 when "Commit Index base actual"=3 then Avg_Actual_YTD_Amount_CNY
	 when "Commit Index base actual"=4 then coalesce(Avg_Actual_YTD_Amount_CNY,0)
     when "Commit Index base actual"=5 then coalesce(last_actual_amount,0)+coalesce(Capex_Forecast_budget,0)
     when "Commit Index base actual"=6 then coalesce(last_actual_amount,0)+coalesce(Lease_Forecast_budget,0)
     when "Commit Index base actual"=7 then Avg_Actual_YTD_Amount_CNY/coalesce(case when YTD_Actual_Sales=0 then 1 else YTD_Actual_Sales end,1)*forecast_sales_amount_YTD
     when "Commit Index base actual"=8 then coalesce(last_actual_amount,0)
     when "Commit Index base actual"=9 then EngineeringAllocations_forecast_manualAdj 
	 when "Commit Index base actual"=10 then AvgOutlook_vs_Actual_baseonActual
	 when "Commit Index base actual"=11 then coalesce(AvgFW_budget_baseonActual,0) else 0
end+coalesce(Manual_Forecast_Adj,0) as Budget_BaseonActual,
case when "Commit Index base forecast"=1 then 0 
     when "Commit Index base forecast"=2 then coalesce(outlook_Budget_Final,0)+coalesce(Labor_Amount_baseOutlook,0) --还有个HC 逻辑待确定
	 when "Commit Index base forecast"=3 then outlook_Budget_Final
	 when "Commit Index base forecast"=4 then coalesce(outlook_Budget_Final,0)
     when "Commit Index base forecast"=5 and Des<>0 then coalesce(last_actual_amount,0) 
     when "Commit Index base forecast"=5 and Des=0 then outlook_Budget_Final  --CF1
     when "Commit Index base forecast"=6 then coalesce(last_actual_amount,0)+coalesce(Capex_Forecast_budget,0)
     when "Commit Index base forecast"=7 then coalesce(last_actual_amount,0)+coalesce(Lease_Forecast_budget,0)
     when "Commit Index base forecast"=8 then last_actual_amount/coalesce(case when last_Actual_salesamount=0 then 1 else last_Actual_salesamount end,1)*forecast_sales_amount
     when "Commit Index base forecast"=9 then coalesce(last_actual_amount,0)
     when "Commit Index base forecast"=10 then EngineeringAllocations_forecast_manualAdj
     when "Commit Index base forecast"=11 then last_actual_amount else 0
end+coalesce(Manual_Forecast_Adj,0) as Budget_Baseonoutlook
from Temp_CommitRno
) a ;

drop table temp_commitrno;

END;
$$;


