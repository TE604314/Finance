
select count(*) from ss_rs_ts_aut_ap_digital_schema.DWS_Finance_FPA_Opex_FinalFact
call ss_rs_ts_aut_ap_digital_schema.sp_Finance_FPA_Opex_ODS_to_DWD_OpexExpenceActualForecast()
CREATE OR REPLACE PROCEDURE ss_rs_ts_aut_ap_digital_schema.sp_Finance_FPA_Opex_ODS_to_DWD_OpexExpenceActualForecast()
LANGUAGE plpgsql
AS $$
BEGIN


delete ss_rs_ts_aut_ap_digital_schema.DWS_Finance_FPA_Opex_FinalFact;
insert into ss_rs_ts_aut_ap_digital_schema.DWS_Finance_FPA_Opex_FinalFact
(
Scenario,
fisical_YearMonth,
Department,
BU_Function,
Team,
CostCenter2,
"Expense level 1",
"Expense level 2",	
Account,	
"Accounts/Cost Elements",
Description,
AccountDescription,	
"Logic",	
"Index",	
"Comments",
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
Amount,
commitRno_last_actual_amount,
commitRno_Labor_Amount_baseActual,
commitRno_Labor_Amount_baseOutlook,
commitRno_Avg_Actual_YTD_Amount_CNY,
commitRno_Manual_Forecast_Adj,
commitRno_Capex_Forecast_budget,
commitRno_Lease_Forecast_budget,
commitRno_YTD_Actual_Sales,
commitRno_last_Actual_salesamount,
commitRno_forecast_sales_amount,
commitRno_outlook_Budget_Final,
commitRno_EngineeringAllocations_forecast_manualAdj,
commitRno_des1,
commitRno_des2,
commitRno_Budget_BaseonActual,
commitRno_Budget_Baseonoutlook,
timestamps
)

SELECT 
Scenario,
fisical_YearMonth as fisical_YearMonth,
'' as Department,
'forcast for identify E Allo' as BU_Function,
Team,
Team as CostCenter2,
"Expense level 1",
"Expense level 2",	
"Acc" as Account,	
"Accounts/Cost Elements",
"Accounts/Cost Elements" as Description,
"Accounts/Cost Elements" as AccountDescription,	
"Commit Logic base actual"||'_'||"Commit Logic base forecast" as "Logic",	
"Commit Logic base actual"||'_'||"Commit Index base forecast" as "Index" ,
'' as "Comments",
0 as budget_index2,
0 asBudget_ActualAvg_budget_index3,
0 as "Budget_Actual_total_index5-6",
0 as Budget_finalmonth_Actual_Index8,
0 as Budget_ActualAvg_Salesv1_index9,
0 as Budget_ActualAvg_Salesv1_index10,
0 as Budget_Capexforecast_index7,
0 as Budget_leasingforecast_index8,
0 as Budget_eachmonth_index12,
0 as budget_Allocations_index13,
cast(CommitRno_ForcastAmount as decimal(25,10)) as Amount,
last_actual_amount as commitRno_last_actual_amount,
Labor_Amount_baseActual as commitRno_Labor_Amount_baseActual,
Labor_Amount_baseOutlook as commitRno_Labor_Amount_baseOutlook,
Avg_Actual_YTD_Amount_CNY  as commitRno_Avg_Actual_YTD_Amount_CNY,
Manual_Forecast_Adj  as commitRno_Manual_Forecast_Adj,
Capex_Forecast_budget  as commitRno_Capex_Forecast_budget,
Lease_Forecast_budget  as commitRno_Lease_Forecast_budget,
YTD_Actual_Sales  as commitRno_YTD_Actual_Sales,
last_Actual_salesamount  as commitRno_last_Actual_salesamount,
forecast_sales_amount  as commitRno_forecast_sales_amount,
outlook_Budget_Final  as commitRno_outlook_Budget_Final,
EngineeringAllocations_forecast_manualAdj  as commitRno_EngineeringAllocations_forecast_manualAdj,
des1  as commitRno_des1,
des2  as commitRno_des2,
Budget_BaseonActual  as commitRno_Budget_BaseonActual,
Budget_Baseonoutlook  as commitRno_Budget_Baseonoutlook,
current_timestamp as timestamps
FROM ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_Forecast_CommitRnoForecast

union all

SELECT 
Scenario,
fisical_YearMonth,
Department,
'forcast for identify E Allo' as BU_Function,
Team,
Team as CostCenter2,
"Expense level 1",
"Expense level 2",	
"Acc" as Account,	
"Accounts/Cost Elements",
"Accounts/Cost Elements" as Description,
"Accounts/Cost Elements" as AccountDescription,	
"Logic",	
"Index",	
"Comments",
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
cast(Budget_Final as decimal(25,10)) as Amount,
0 as commitRno_last_actual_amount,
0 as commitRno_Labor_Amount_baseActual,
0 as commitRno_Labor_Amount_baseOutlook,
0  as commitRno_Avg_Actual_YTD_Amount_CNY,
0  as commitRno_Manual_Forecast_Adj,
0  as commitRno_Capex_Forecast_budget,
0  as commitRno_Lease_Forecast_budget,
0  as commitRno_YTD_Actual_Sales,
0  as commitRno_last_Actual_salesamount,
0  as commitRno_forecast_sales_amount,
0  as commitRno_outlook_Budget_Final,
0  as commitRno_EngineeringAllocations_forecast_manualAdj,
null  as commitRno_des1,
null  as commitRno_des2,
0  as commitRno_Budget_BaseonActual,
0  as commitRno_Budget_Baseonoutlook,
current_timestamp as timestamps
FROM ss_rs_ts_aut_ap_digital_schema.DWD_Finance_FPA_Opex_Forecast_FactVersion1

union all

select
fisical_Year||' 12+0' as Scenario,
fisical_YearMonth,
"Department",
BU_Function,
"Team",
CostCenter2,
'' as "Expense level 1",
'' as "Expense level 2",
Account,
Account as "Accounts/Cost Elements",
Description,
AccountDescription,
'' as "Logic",	
'' as "Index",	
'' as "Comments",
0 as budget_index2,
0 as Budget_ActualAvg_budget_index3,
0 as "Budget_Actual_total_index5-6",
0 as Budget_finalmonth_Actual_Index8,
0 as Budget_ActualAvg_Salesv1_index9,
0 as Budget_ActualAvg_Salesv1_index10,
0 as Budget_Capexforecast_index7,
0 as Budget_leasingforecast_index8,
0 as Budget_eachmonth_index12,
0 as budget_Allocations_index13,
cast(Amount_CNY as decimal(25,10)) as Amount,
0 as commitRno_last_actual_amount,
0 as commitRno_Labor_Amount_baseActual,
0 as commitRno_Labor_Amount_baseOutlook,
0  as commitRno_Avg_Actual_YTD_Amount_CNY,
0  as commitRno_Manual_Forecast_Adj,
0  as commitRno_Capex_Forecast_budget,
0  as commitRno_Lease_Forecast_budget,
0  as commitRno_YTD_Actual_Sales,
0  as commitRno_last_Actual_salesamount,
0  as commitRno_forecast_sales_amount,
0  as commitRno_outlook_Budget_Final,
0  as commitRno_EngineeringAllocations_forecast_manualAdj,
null  as commitRno_des1,
null  as commitRno_des2,
0  as commitRno_Budget_BaseonActual,
0  as commitRno_Budget_Baseonoutlook,
current_timestamp as timestamps
from ss_rs_ts_aut_ap_digital_schema.DWS_Finance_FPA_Opex_ActualInternal_FinalActual;



create temporary table Temp_Scenario as 
select 
distinct
b.Scenario,
fiscal_year_id,
fiscal_quarter_id,
fiscal_quarter_code,
a.fiscal_year_month_id,
calendar_date,
c."Index",
timestamps
from 
(
select 
fiscal_year_id,
fiscal_quarter_id,
fiscal_quarter_code,
fiscal_year_month_id,
to_char(max(calendar_date),'YYYYMMDD') as calendar_date,
current_timestamp as timestamps
from ss_rs_ts_aut_ap_digital_db."master_data"."dimension_date"  
where fiscal_year_id>=2015
group by 
fiscal_year_id,
fiscal_quarter_id,
fiscal_quarter_code,
fiscal_year_month_id
) a

inner join

(
select
distinct
Scenario,
fisical_YearMonth as YearMonth
FROM ss_rs_ts_aut_ap_digital_schema.DWS_Finance_FPA_Opex_FinalFact
) b
on a.fiscal_year_month_id=b.YearMonth

left join
(
select 
scenario,
max("index") as "index"
from s3dataschema.ods_Finance_FPA_Opex_ScenarioMap
group by scenario
) c
on split_part(b.Scenario,' ',2)=c.scenario;


delete ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_Opex_Calendar;
insert into ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_Opex_Calendar
(
Parent_Scenario,
Child_Scenario,
fiscal_year_id,
fiscal_quarter_id,
fiscal_quarter_code,
fiscal_year_month_id,
calendar_date,
"Index",
timestamps
)
select
a.Scenario as Parent_Scenario,
b.Scenario as Child_Scenario,
fiscal_year_id,
fiscal_quarter_id,
fiscal_quarter_code,
fiscal_year_month_id,
calendar_date,
a."Index",
current_timestamp as timestamps
from 
(
select
distinct
Scenario,
left(Scenario,4)||right(CAST("Index" AS INT)+100,2) as Scenario_YM,
"Index"
from Temp_Scenario
where scenario not like '%12+0%' and scenario not like '%Outlook%'
) a 

left join 

(
select
distinct
Scenario,
fiscal_year_id,
fiscal_quarter_id,
fiscal_quarter_code,
fiscal_year_month_id,
calendar_date,
"Index"
from Temp_Scenario
where scenario like  '%12+0%'
) b 
on left(a.Scenario,4)=left(b.scenario,4)
where b.fiscal_year_month_id <= a.Scenario_YM

union

select
Scenario as Parent_Scenario,
Scenario as Child_Scenario,
fiscal_year_id,
fiscal_quarter_id,
fiscal_quarter_code,
fiscal_year_month_id,
calendar_date,
"Index",
current_timestamp as timestamps
from Temp_Scenario;


create temporary table temp_Comparison as 
select
Scenario_A||' vs '||Scenario_B AS Title,
Scenario_A,
Scenario_B
from 
(
select 
distinct 
left(Scenario,4) as years,
1 AS id,
Scenario as Scenario_A
from Temp_Scenario
) a

LEFT JOIN 

(
select 
distinct
left(Scenario,4) as years,
1 AS id,
Scenario AS Scenario_B from Temp_Scenario
) b
on a.id=b.id and a.years=b.years and a.Scenario_A<>b.Scenario_B
WHERE Title is not null;

DROP TABLE Temp_Scenario;

delete ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_Opex_ScenarioforUI;
insert into ss_rs_ts_aut_ap_digital_schema.Dim_Finance_FPA_Opex_ScenarioforUI
(
Title,
present_Scenario,
Scenario_A_link,
Scenario_B_link,
Ranks
)

select
Title,
present_Scenario,
Scenario_A_link,
Scenario_B_link,
Ranks
from
(
select 
Title,
Scenario_A||'/' as present_Scenario,
Scenario_A as Scenario_A_link,
Scenario_B as Scenario_B_link,
1 as Ranks
from temp_Comparison

UNION ALL

select 
Title,
Scenario_B as present_Scenario,
Scenario_A as Scenario_A_link,
Scenario_B as Scenario_B_link,
2 as Ranks
from temp_Comparison


UNION ALL

select 
Title,
'Gap' as present_Scenario,
Scenario_A as Scenario_A_link,
Scenario_B as Scenario_B_link,
3 as Ranks
from temp_Comparison
) a ;

drop table temp_Comparison;
END;
$$;
