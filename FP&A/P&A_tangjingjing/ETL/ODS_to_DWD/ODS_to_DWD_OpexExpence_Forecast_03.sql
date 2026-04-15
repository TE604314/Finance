drop table temp_budgetactualavg 

call  ss_rs_ts_aut_ap_digital_schema.sp_Finance_FPA_Opex_ODS_to_DWD_OpexExpenceForecastv1()

select * from Temp_TBR

CREATE OR REPLACE PROCEDURE ss_rs_ts_aut_ap_digital_schema.sp_Finance_FPA_Opex_ODS_to_DWD_OpexExpenceForecastv1()
LANGUAGE plpgsql
AS $$
BEGIN


create temporary table Temp_TBR AS 
select 
    1 as id,
    left(to_char(expiration_dt,'YYYYMMDD'),4) as fifiscal_year,
    left(to_char(expiration_dt,'YYYYMMDD'),8) as fifiscal_year_begin_date ,
	left(to_char(expiration_dt,'YYYYMMDD'),8) as fiscal_year_end_date,
    max(left(to_char(expiration_dt,'YYYYMMDD'),8)) over(partition by 1) as flag,
    currency_cde,
    "rate_from_us_mult_fctr" as exchange_rate_USD
FROM "ss_rs_ts_aut_ap_digital_db"."master_data"."dimension_currency_exchange_rates_all"
WHERE currency_exchange_rate_cde=1 and currency_cde in ('CNY','TWD','USD') and 
left(to_char(expiration_dt,'YYYYMMDD'),4) in (select max(left(YearMonth,4)) from ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_ZB12);

--base
create temporary table Temp_base as
select 
a.Scenario,
a.Fyear||b."index" as ScenarioYM,
c.fisical_YearMonth,
c.Department,
c."Team",
c."Team(Index)",
c.currency_cde,
c."Account",
c."index",
c."NA H. 3 Name", 	
c."NA H. 4 Name",
c."Accounts/Cost Elements",
c.amount
from
(
select
replace(Fyear,'FY','') as Fyear,
replace(Fyear,'FY','')||' '||Des2 as Scenario,
Des2
--2025 as Fyear,
--'2025 AugRnO' as Scenario,
--'AugRnO' as Des2
from s3dataschema.ods_Finance_FPA_Opex_parameter 
where fyear<>'Fyear' and replace(Fyear,'FY','')>2025
) a

left join 

(
select
distinct
1 as id,
scenario,
Des,
right(cast("index" as INT)+100,2) as "index"
from s3dataschema.ods_Finance_FPA_Opex_ScenarioMap
where scenario<>'Scenario'
) b
on a.Des2=b.scenario

left join 

(
select
scenario,
fisical_YearMonth,
"Team" as Department,
"Team",
"Team" as "Team(Index)",
'CNY' as currency_cde,
"Acc" as "Account",
"index",
null as "NA H. 3 Name", 	
null as "NA H. 4 Name",
"Acc" as "Accounts/Cost Elements",
CommitRno_ForcastAmount as amount
from ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_Forecast_CommitRnoForecast
) c
on a.scenario=c.scenario

union all

select 
a.Scenario,
a.Fyear||b."index" as ScenarioYM,
c.fisical_YearMonth,
c.Department,
c."Team",
c."Team(Index)",
c.currency_cde,
c."Account",
c."index",
c."NA H. 3 Name", 	
c."NA H. 4 Name",
c."Accounts/Cost Elements",
c.amount
from
(
select
replace(Fyear,'FY','') as Fyear,
replace(Fyear,'FY','')||' '||Des2 as Scenario,
Des2
--2025 as Fyear,
--'2025 AugRnO' as Scenario,
--'AugRnO' as Des2
from s3dataschema.ods_Finance_FPA_Opex_parameter 
where fyear<>'Fyear' and replace(Fyear,'FY','')>2025
) a

left join 

(
select
distinct
1 as id,
scenario,
Des,
right(cast("index" as INT)+100,2) as "index"
from s3dataschema.ods_Finance_FPA_Opex_ScenarioMap
where scenario<>'Scenario'
) b
on a.Des2=b.scenario

left join

(
select
fisical_Year,
fisical_YearMonth,
"Department",
"Team",
"Team" as "Team(Index)",
'CNY' as currency_cde,
Account,
"Team" as "Index",
null as "NA H. 3 Name", 	
null as "NA H. 4 Name",
Account as "Accounts/Cost Elements",
Amount_CNY as amount
from ss_rs_ts_aut_ap_digital_schema.DWS_Finance_FPA_Opex_ActualInternal_FinalActual
) c
on a.Fyear=c.fisical_Year and left(c.fisical_YearMonth,6)<=left(a.Fyear||b."index",6);


create temporary table  Temp_AUTExpense_step1 as 
select
1 as ID,
Department,
Team,
"Team(Index)",
currency_cde,
"Account",	
"Index",	
"NA H. 3 Name", 	
"NA H. 4 Name", 	
"Accounts/Cost Elements",	
replace(replace(fisical_YearMonth,'fy','20'),'p','') as fisical_YearMonth,
sum(amount) as amount
from
(
select
Department,
Team,
"Team(Index)",
case when left("Team(Index)",2)='TW' then 'TWD'
     when left("Team(Index)",2)='HK' then 'USD' else 'CNY'
END as currency_cde,
"Account",	
"Index",	
"NA H. 3 Name", 	
"NA H. 4 Name", 	
"Accounts/Cost Elements",	 
fisical_YearMonth,
case when amount is null or amount='' then 0 else cast(amount as float) end as amount
from s3dataschema.ods_Finance_FPA_Opex_TempAUTExpense as a 
unpivot(amount for fisical_YearMonth in 
(FY25P01,	 
FY25P02,	 
FY25P03,	 
FY25P04,	 
FY25P05,	 
FY25P06,	 
FY25P07,	 
FY25P08,	 
FY25P09,	 
FY25P10,	 
FY25P11,	 
FY25P12 ))
where "Team"<>'Team' 
) b
group by 
Department,
Team,
"Team(Index)",
currency_cde,
"Account",	
"Index",	
"NA H. 3 Name", 	
"NA H. 4 Name", 	
"Accounts/Cost Elements",	 
fisical_YearMonth
having sum(amount)<>0

union all


select 
1 as ID,
Department,
Team,
"Team(Index)",
currency_cde,
"Account",	
"Index",	
"NA H. 3 Name", 	
"NA H. 4 Name", 	
"Accounts/Cost Elements",	
fisical_YearMonth,
sum(coalesce(amount,0)) as amount
from Temp_base
group by 
Department,
Team,
"Team(Index)",
currency_cde,
"Account",	
"Index",	
"NA H. 3 Name", 	
"NA H. 4 Name", 	
"Accounts/Cost Elements",	
fisical_YearMonth
having sum(coalesce(amount,0)) <>0;

drop table Temp_base;


create temporary table  Temp_AUTExpense as 
select
Department,
Team,
"Team(Index)",
currency_cde,
"Account",	
"Index",	
"NA H. 3 Name", 	
"NA H. 4 Name", 	
"Accounts/Cost Elements",	
fisical_YearMonth,
c.cny_usd_rate,
c.twd_usd_rate,
amount as Original_amount,
case when currency_cde='USD' then amount*c.cny_usd_rate
     when currency_cde='TWD' then amount/c.twd_usd_rate*c.cny_usd_rate ELSE
      Original_amount END AS Amount_CNY
from Temp_AUTExpense_step1 a 

left join 

(
select
1 AS ID,
sum(cny_usd_rate) as cny_usd_rate,
sum(twd_usd_rate) as twd_usd_rate
from
(
select 
--fifiscal_year,
 1 AS ID,
 case when currency_cde='CNY' then exchange_rate_USD else 0 end  AS cny_usd_rate,
 case when currency_cde='TWD' then exchange_rate_USD else 0 end AS twd_usd_rate
 from Temp_TBR
 ) a1
 group by ID
) c 
on a.ID=c.ID;


drop table Temp_AUTExpense_step1;
drop table Temp_TBR;

--全集
create temporary table  Temp_all_dim as 
select
fisical_Year,
fisical_NextYear,
fisical_YearQuarter,
fisical_YearMonth,
Team,
Account
from
(
select
distinct
left(fisical_YearMonth,4) as fisical_Year,
cast(cast(left(fisical_YearMonth,4) as int)+1 as varchar(255)) as fisical_NextYear,
left(fisical_YearMonth,4)||'Q'||ceil(right(fisical_YearMonth,2)/3) as fisical_YearQuarter,
fisical_YearMonth
from Temp_AUTExpense
) a1

left join

(
select 
distinct
Team,
Account
from Temp_AUTExpense 
)
on 1=1;

--HR 提供
Create temporary table temp_Opexexpensebudget as 
select
Year||right(fisical_Month,2) as fisical_YearMonth,
department,
"GL Acc",
budget
from 
(
select
split_part(filename,'_',1) as Scenario,
left(filename,4) as Year,
department,
"GL Acc",
Oct AS P01,
Nov as P02,
"Dec" as P03,
Jan as P04,
Feb as P05,
Mar as P06,
Apr as P07,
May as P08,
Jun as P09,
Jul as P10,
Aug as P11,
Sep as P12
from s3dataschema.ods_Finance_FPA_Opex_Opexexpensebudget 
where "index"<>'INDEX' and  filename like '%Outlook%'
) as a 
unpivot(budget for fisical_Month in ("P01","P02",P03,P04,P05,P06,P07,P08,P09,P10,P11,P12));

--Capex forecast
create temporary table Temp_CapexForecast as 
select 
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
split_part(filename,'_',1) as Scenario,
left(filename,4) as Year,
Filename,
--"CER / external order number",
--"Description",
--"Cost Center",
--"IO/PO Responsible",
"Department",
Acc,
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
from s3dataschema.ods_Finance_FPA_Opex_CapexForecast
where  sheetname<>'sheetname' and LOWER(filename) like '%outlook%'
) as a 
unpivot(budget for fisical_Month in ("P01","P02",P03,P04,P05,P06,P07,P08,P09,P10,P11,P12))
where budget<>0;

--LeaseForecast
create temporary table Temp_LeaseForecast as 
select 
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
split_part(filename,'_',1) as Scenario,
left(filename,4) as Year,
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
where index<>'Index' and filename like '%Outlook%'
) as a 
unpivot(budget for fisical_Month in ("P01","P02",P03,P04,P05,P06,P07,P08,P09,P10,P11,P12))
where budget<>0;


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

--去年by ym 均值
create temporary table Temp_BudgetActualAvg as 
select 
fisical_Year,
fisical_NextYear,
Department,
Team,
"Account",	
Budget_ActualAvg,
Budget_Actual_total,
Budget_finalmonth_Actual,
Budget_ActualAvg*(1+"YOY Increase%") as Budget_ActualAvg_Salesv1,
Budget_ActualAvg*(1+"YOY Increase%"/2) as Budget_ActualAvg_Salesv2,
"YOY Increase%"
from 

(
select 
left(fisical_YearMonth,4) as fisical_Year,
cast(cast(left(fisical_YearMonth,4) as int)+1 as varchar(255)) as fisical_NextYear,
Department,
Team,
"Account",	
sum(Amount_CNY)/12 as Budget_ActualAvg,
sum(Amount_CNY) as Budget_Actual_total,
sum(case when right(fisical_YearMonth,2)=12 then Amount_CNY else 0 end) as Budget_finalmonth_Actual
from Temp_AUTExpense 
group by left(fisical_YearMonth,4),Department,"Account",Team
) a 


left join 
(
select 
left("Fyear",4) as Fyear,
max("Sales") as Sales,
max(cast("YOY Increase%" as float)) as "YOY Increase%"
from s3dataschema.ods_Finance_FPA_Opex_Salesbudget
where fyear<>'Fyear'
group by Fyear
) b 
on a.fisical_NextYear=b.Fyear;

--去年by Q 均值
create temporary table Temp_AvgByQ as 
select
a1.fisical_Year,
a1.fisical_NextYear,
a1.fisical_YearQuarter,
a1.fisical_YearMonth,
a1.Team,
a1.Account,
b.count_byM,
b.count_byQ,
c."YOY Increase%",
a.Budget_Actual_total,
sum(a.Budget_Actual_total*(1+COALESCE(c."YOY Increase%",0)/2))over(partition by a1.fisical_YearQuarter,a1.Team,a1.Account) as Budget_Actual_byQ,
sum(a.Budget_Actual_total*(1+COALESCE(c."YOY Increase%",0)/2))over(partition by a1.fisical_YearQuarter,a1.Team,a1.Account) /b.count_byQ*b.count_byM AS Budget_eachmonth
from 

(
select
fisical_Year,
fisical_NextYear,
fisical_YearQuarter,
fisical_YearMonth,
Team,
Account
from Temp_all_dim
) a1

left join 
(
select 
--left(fisical_YearMonth,4) as fisical_Year,
--cast(cast(left(fisical_YearMonth,4) as int)+1 as varchar(255)) as fisical_NextYear,
--left(fisical_YearMonth,4)||'Q'||ceil(right(fisical_YearMonth,2)/3) as fisical_YearQuarter,
fisical_YearMonth,
Team,
Account,
sum(Amount_CNY) as Budget_Actual_total
from Temp_AUTExpense 
group by fisical_YearMonth,Team,Account
) a 
on a1.fisical_YearMonth=a.fisical_YearMonth and a1.Team=a.Team and a1.Account=a.Account

left join

(
select
distinct
fiscal_year_id||fiscal_quarter_code as fisical_YearQuarter,
fiscal_year_id||right(fiscal_month_of_year_id+100,2) as fisical_YearMonth,
count(fiscal_week_of_year_id)over(partition by fiscal_year_id,fiscal_quarter_code) as count_byQ,
count(fiscal_week_of_year_id)over(partition by fiscal_year_id,fiscal_month_of_year_id) as count_byM
from
(
select 
distinct  
fiscal_year_id,
fiscal_quarter_code,
fiscal_month_of_year_id,
fiscal_week_of_year_id
from ss_rs_ts_aut_ap_digital_db."master_data"."dimension_date"
where fiscal_year_id>=2025
) b1
) b
on a1.fisical_YearMonth=b.fisical_YearMonth

left join 
(
select 
left("Fyear",4) as fisical_Year,
--max("Sales") as Sales,
max(cast("YOY Increase%" as float)) as "YOY Increase%"
from s3dataschema.ods_Finance_FPA_Opex_Salesbudget
where fyear<>'Fyear'
group by Fyear
) c
on a1.fisical_NextYear=c.fisical_Year
;

--EngineeringAllocations 

create temporary table Temp_EngineeringAllocations as 
select 
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
where acc<>'Acc' AND Filename LIKE '%Outlook%'
) as a 
unpivot(budget for fisical_Month in ("P01","P02",P03,P04,P05,P06,P07,P08,P09,P10,P11,P12))
where budget<>0 and  budget is not null and budget<>'' ;

delete ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_Forecast_FactStep1;
insert into ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_Forecast_FactStep1
(
fisical_YearMonth,
Department,
Team,
"Expense level 1",
"Expense level 2",	
"Acc",	
"Accounts/Cost Elements",	
"Logic",	
"Index",	
"Comments",
Manual_Forecast_Adj,
budget_index2,
Budget_ActualAvg_budget_index3,
"Budget_Actual_total_index5-6",
Budget_finalmonth_Actual_Index8,
Budget_ActualAvg_Salesv1_index9,
Budget_ActualAvg_Salesv1_index10,
Budget_Capexforecast_index7,
Budget_leasingforecast_index8,
Budget_eachmonth_index12,
budget_Allocations_index13,
timestamps
)
select 
c.fisical_YearMonth,
a.Department,
a.Team,
"Expense level 1",
"Expense level 2",	
b."Acc",	
"Accounts/Cost Elements",	
"Logic",	
"Index",	
"Comments",
c1.Manual_Forecast_Adj,
d.budget as budget_index2,
e.Budget_ActualAvg as Budget_ActualAvg_budget_index3,
e.Budget_Actual_total as "Budget_Actual_total_index5-6",
e.Budget_finalmonth_Actual as Budget_finalmonth_Actual_Index8,
e.Budget_ActualAvg_Salesv1 as Budget_ActualAvg_Salesv1_index9,
e.Budget_ActualAvg_Salesv2 as Budget_ActualAvg_Salesv1_index10,
f.budget as Budget_Capexforecast_index7,
g.budget as Budget_leasingforecast_index8,
h.Budget_eachmonth as Budget_eachmonth_index12,
i.budget as budget_Allocations_index13,
current_timestamp as timestamps
from 
(
select
distinct
1 as id,
Department,
Team
from Temp_AUTExpense
where Department<>Team
) a 
left join 
(
select 
1 as id,
start_fiscalYear,
End_fiscalYear,
"Expense level 1",
"Expense level 2",	
"Acc",	
"Accounts/Cost Elements",	
"Logic",	
"Index",	
"Comments"
from s3dataschema.ods_Finance_FPA_Opex_ForecastLogic
where Acc<>'Acc'
) b
on a.id=b.id

left join
(
select
distinct
fisical_YearMonth
from temp_Opexexpensebudget
) c
on left(c.fisical_YearMonth,4) between b.start_fiscalYear and b.End_fiscalYear

left join 
--Manual Adj
(
select
F_YearMonth,
Team,
Acc,
sum(coalesce(cast(amount as float),0)) as Manual_Forecast_Adj
from Temp_Forecast_ManualAdj
where coalesce(cast(amount as float),0) <>0 and lower(Scenario) like '%outlook%'
group by 
F_YearMonth,
Team,
Acc
) c1
on  c.fisical_YearMonth=c1.F_YearMonth and a.Team=c1.Team and b.Acc=c1.Acc


--HR 提供
left join 
(
select
fisical_YearMonth,
department as Team,
"GL Acc",
sum(budget) as budget
from temp_Opexexpensebudget
where department is not null and department<>''
GROUP BY fisical_YearMonth,department,"GL Acc"
) d
on d.fisical_YearMonth=c.fisical_YearMonth and a.Team=d.Team and b.Acc=d."GL Acc"


--BudgetActualAvg

left join 
(
select 
--fisical_Year,
fisical_NextYear,
--Department,
Team,
"Account",	
max(Budget_ActualAvg) as Budget_ActualAvg,
max(Budget_Actual_total) as Budget_Actual_total,
max(Budget_finalmonth_Actual) as Budget_finalmonth_Actual,
max(Budget_ActualAvg_Salesv1) as Budget_ActualAvg_Salesv1,
max(Budget_ActualAvg_Salesv2) as Budget_ActualAvg_Salesv2
from Temp_BudgetActualAvg
group by
fisical_NextYear,Team,
"Account"
) e 
on e.fisical_NextYear =left(c.fisical_YearMonth,4) and a.Team=e.Team and b.Acc=e."Account"

--CapexForecast
left join 
(
select 
fisical_YearMonth,
"Department" as Team,
Acc,
sum(budget) as budget
from Temp_CapexForecast
group by fisical_YearMonth,Department,Acc
) f
on f.fisical_YearMonth=c.fisical_YearMonth and a.Team=f.Team and b.Acc=f.Acc

--LeaseForecast
left join 
(
select 
fisical_YearMonth,
"Department" as Team,
Acc,
sum(budget) as budget
from Temp_LeaseForecast
group by fisical_YearMonth,Department,Acc
) g
on g.fisical_YearMonth=c.fisical_YearMonth and a.Team=g.Team and b.Acc=g."Acc"

left join 
(
select
replace(fisical_YearMonth,fisical_Year,fisical_NextYear) as Next_fisical_YearMonth,
Team,
Account,
sum(Budget_eachmonth) as Budget_eachmonth
from Temp_AvgByQ
group by replace(fisical_YearMonth,fisical_Year,fisical_NextYear),Team,
Account
) h 
on h.Next_fisical_YearMonth=c.fisical_YearMonth and a.Team=h.Team and b.Acc=h."Account"

left join 

(
select 
fisical_YearMonth,
Department as Team,
Acc	,	
sum(budget) as budget
from Temp_EngineeringAllocations
group by fisical_YearMonth,
Acc,Department
) i
on i.fisical_YearMonth=c.fisical_YearMonth  and b.Acc=i."Acc" and a.Team=i.Team;

drop table Temp_Forecast_ManualAdj;
drop table Temp_AUTExpense;
drop table temp_Opexexpensebudget;
drop table Temp_CapexForecast;
drop table Temp_LeaseForecast;
drop table Temp_BudgetActualAvg;
drop table Temp_AvgByQ;
drop table Temp_EngineeringAllocations;


delete ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_Forecast_FactVersion1;
insert into ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_Forecast_FactVersion1
(
Scenario,
fisical_YearMonth,
Department,
Team,
"Expense level 1",
"Expense level 2",	
"Acc",	
"Accounts/Cost Elements",	
"Logic",	
"Index",	
"Comments",
Manual_Forecast_Adj,
budget_index2,
Budget_ActualAvg_budget_index3,
"Budget_Actual_total_index5-6",
Budget_finalmonth_Actual_Index8,
Budget_ActualAvg_Salesv1_index9,
Budget_ActualAvg_Salesv1_index10,
Budget_Capexforecast_index7,
Budget_leasingforecast_index8,
Budget_eachmonth_index12,
budget_Allocations_index13,
Budget_Final,
timestamps
)
select 
concat(left(fisical_YearMonth,4),' Outlook') as Scenario,
fisical_YearMonth,
Department,
Team,
"Expense level 1",
"Expense level 2",	
"Acc",	
"Accounts/Cost Elements",	
"Logic",	
"Index",	
"Comments",
Manual_Forecast_Adj,
budget_index2,
Budget_ActualAvg_budget_index3,
"Budget_Actual_total_index5-6",
Budget_finalmonth_Actual_Index8,
Budget_ActualAvg_Salesv1_index9,
Budget_ActualAvg_Salesv1_index10,
Budget_Capexforecast_index7,
Budget_leasingforecast_index8,
Budget_eachmonth_index12,
budget_Allocations_index13,
coalesce(case when "Index"=1 then 0
     when "Index"=2 then budget_index2
     when "Index"=3 then Budget_ActualAvg_budget_index3
     when "Index"=4 and Team='TW Sales' then Budget_ActualAvg_budget_index3
     when "Index"=5 and Team='SH G&A' and cast(right(fisical_YearMonth,2) as int)=4 then "Budget_Actual_total_index5-6"
	 when "Index"=5 and Team<>'SH G&A' then Budget_ActualAvg_budget_index3
	 when "Index"=6 and Team='SH G&A' and cast(right(fisical_YearMonth,2)as int)=4 then "Budget_Actual_total_index5-6"
     when "Index"=6 and Team<>'SH G&A' and coalesce(budget_index2,0)<>0 then budget_index2
	 when "Index"=6 and Team<>'SH G&A' and coalesce(budget_index2,0)=0 then Budget_ActualAvg_budget_index3
     when "Index"=7 then coalesce(Budget_finalmonth_Actual_Index8,0)+coalesce(Budget_Capexforecast_index7,0)
	 when "Index"=8 then coalesce(Budget_finalmonth_Actual_Index8,0)+coalesce(Budget_leasingforecast_index8,0)
	 when "Index"=9 then coalesce(Budget_ActualAvg_Salesv1_index9,0)
	 when "Index"=10 then coalesce(Budget_ActualAvg_Salesv1_index10,0)
	 when "Index"=11 and  coalesce(budget_index2,0)<>0 then budget_index2
	 when "Index"=11 and  coalesce(budget_index2,0)=0 then coalesce(Budget_ActualAvg_Salesv1_index10,0)
	 when "Index"=12 then coalesce(Budget_eachmonth_index12,0)
	 when "Index"=13 then coalesce(budget_Allocations_index13,0)
     when "Index"=14 and Team='TW Sales' then coalesce(Budget_ActualAvg_budget_index3,0)
	 when "Index"=14 and Team<>'TW Sales' then coalesce(budget_index2,0)
else 
0
end,0)+coalesce(Manual_Forecast_Adj,0) as Budget_Final,
current_timestamp as timestamps
from ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_Forecast_FactStep1;


drop table Temp_all_dim;

END;
$$;

select distinct fisical_YearMonth 
from ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_Forecast_FactVersion1
where Acc='6023009' limit 100

select *
from ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_Forecast_FactVersion1
where fisical_YearMonth=202601 and "Acc"='9000001' and Team='SH RD'

select *
from ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_Forecast_FactStep1
where fisical_YearMonth=202601 and "Acc"='9000001' and Team='SH RD'

select 
left(fisical_YearMonth,4) as fisical_Year,
cast(cast(left(fisical_YearMonth,4) as int)+1 as varchar(255)) as fisical_NextYear,
Department,
Team,
"Account",	
sum(Amount_CNY)/12 as Budget_ActualAvg,
sum(Amount_CNY) as Budget_Actual_total,
sum(case when right(fisical_YearMonth,2)=12 then Amount_CNY else 0 end) as Budget_finalmonth_Actual
from Temp_AUTExpense 
where  "Account"='6081360' and Department='G&A'
group by left(fisical_YearMonth,4),Department,"Account",Team


select * from Temp_BudgetActualAvg
where  "Account"='6081360' and Department='G&A'

select
1 as ID,
Department,
Team,
"Team(Index)",
currency_cde,
"Account",	
"Index",	
"NA H. 3 Name", 	
"NA H. 4 Name", 	
"Accounts/Cost Elements",	
fisical_YearMonth,
amount
from Temp_AUTExpense_step1

select * from ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_Forecast_FactVersion1
where Department='SH RD' and "Acc"='6051000'

s3dataschema.ods_Finance_FPA_Opex_Opexexpensebudget 

select 
--fisical_Year,
fisical_NextYear,
--Department,
Team,
"Account",	
max(Budget_ActualAvg) as Budget_ActualAvg,
max(Budget_Actual_total) as Budget_Actual_total,
max(Budget_finalmonth_Actual) as Budget_finalmonth_Actual,
max(Budget_ActualAvg_Salesv1) as Budget_ActualAvg_Salesv1,
max(Budget_ActualAvg_Salesv2) as Budget_ActualAvg_Salesv2
from Temp_BudgetActualAvg
where fisical_NextYear=2026 and Team='TW Sales' and Account='6051104'
group by
fisical_NextYear,Team,
"Account"

select * from Temp_AUTExpense
where fisical_yearmonth=202501 and Team='TW Sales' and Account='6051104'

select * from Temp_TBR
